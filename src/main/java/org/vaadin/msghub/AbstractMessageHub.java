package org.vaadin.msghub;

import com.vaadin.server.ClientConnector;
import com.vaadin.server.VaadinSession;
import com.vaadin.ui.UI;
import java.io.Serializable;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.http.HttpSessionActivationListener;
import javax.servlet.http.HttpSessionEvent;

/**
 * JMSContext (injectable in Java EE) don't allow setListener :-( Thus,
 * receiving messages to individual users via JMS must be implemented with
 * application managed approach.
 * <p>
 * The class implements session activation listener and saves itself to session.
 * Relevant fields are transient and the class thus stores pretty much nothing
 * in the session, but they are reconnected when the session is revived on a new
 * cluster node. Note, that the current solution loses messages that are
 * received during "hibernation". To workaround this, the board state should
 * also be stored in an EJB and the initial state should be read from that when
 * (re)activating.
 *
 * @param <T> the type of the UI that this hub handles.
 */
public abstract class AbstractMessageHub<T extends UI> implements
        HttpSessionActivationListener,
        MessageListener, Serializable {

    private transient T ui;
    private transient JMSContext c;
    private JMSProducer producer;
    private Topic chatTopic;

    private AbstractMessageHub() {

    }

    /**
     * @return the class of the UI type that this hub handles
     */
    protected abstract Class<T> getUiClass();

    public AbstractMessageHub(T mainUI) {
        this.ui = (T) mainUI;
        registerActivationListener();
    }

    private void registerActivationListener() {
        ui.getSession().getSession().setAttribute(getAttributeName(), this);
    }

    protected String getAttributeName() {
        return getClass().getName() + ui.getUIId();
    }

    public void startListening() {
        try {
            InitialContext ctx = new InitialContext();
            chatTopic = (Topic) ctx.lookup(getTopicName());
            ConnectionFactory factory
                    = (ConnectionFactory) ctx.lookup(getConnectionFactoryJndiLookup());
            c = factory.createContext();
            c.createConsumer(chatTopic).setMessageListener(this);
            c.start();
            ui.addDetachListener(new ClientConnector.DetachListener() {
                @Override
                public void detach(ClientConnector.DetachEvent event) {
                    c.close();
                    deregisterActivationListener();
                }
            });

        } catch (NamingException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public synchronized void sendText(String text) {
        getProducer().send(chatTopic, text);
    }
    
    public synchronized void sendObject(Serializable object) {
        getProducer().send(chatTopic, object);
    }
    
    private JMSProducer getProducer() {
        if(producer == null) {
            producer = c.createProducer();
        }
        return producer;
    }
    
    protected String getConnectionFactoryJndiLookup() {
        return "ConnectionFactory";
    }

    abstract protected String getTopicName();

    public T getUi() {
        return ui;
    }

    @Override
    public synchronized void onMessage(final Message message) {
        getUi().access(new Runnable() {
            @Override
            public void run() {
                handleMessage(getUi(), message);
            }
        });
    }

    /**
     * This method passes message to ui instance. The call is automatically
     * synchronized with UI.access(Runnable r).
     *
     * @param ui
     * @param message
     */
    protected abstract void handleMessage(T ui, Message message);

    @Override
    public void sessionWillPassivate(HttpSessionEvent se) {
        /* cluse the JMS connection as we cannot serialize that, nor update the
         * UI while it is on disk or moved another node. */
        c.close();
    }

    @Override
    public void sessionDidActivate(HttpSessionEvent se) {
        /*
         * This is called by servlet when/if session is transferred to a different
         * node in a cluster. In these cases, we must reconnect a new JMS listener to UI.
         */
        for (VaadinSession vaadinSession : VaadinSession.getAllSessions(se.
                getSession())) {
            for (UI ui : vaadinSession.getUIs()) {
                if (getClass().isAssignableFrom(ui.getClass())) {
                    this.ui = (T) ui;
                    startListening();
                    return;
                }
            }
        }
        throw new RuntimeException("UI not found!");
    }

    private void deregisterActivationListener() {
        ui.getSession().getSession().removeAttribute(getAttributeName());
    }

}
