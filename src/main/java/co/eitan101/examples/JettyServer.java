package co.eitan101.examples;

import java.util.logging.ConsoleHandler;
import javax.websocket.server.ServerContainer;
import org.eclipse.jetty.server.Handler;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;

public class JettyServer {

    public static void main(String[] args) {
        PmQueryServerExample.gePmQueryServer().put("all", pm->pm.getName().startsWith("a"));
        runHttpServer(WsTest.class, "index.html");
    }

    private static void runHttpServer(Class<WsTest> wsClass, String welcomeFile) {
        Server server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(8080);
        server.addConnector(connector);

        final ResourceHandler rh = new ResourceHandler();
        rh.setDirectoriesListed(true);
        rh.setWelcomeFiles(new String[]{ welcomeFile});
        rh.setResourceBase(".");
        final HandlerList handlers = new HandlerList();
        final ContextHandler resourceCtx = new ContextHandler();
        resourceCtx.setContextPath("/files/");
        resourceCtx.setHandler(rh);
        ServletContextHandler wsContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
        wsContext.setContextPath("/data");
        handlers.setHandlers(new Handler[] {resourceCtx, wsContext,new DefaultHandler()});
        server.setHandler(handlers);

        try {
            ServerContainer wscontainer = WebSocketServerContainerInitializer.configureContext(wsContext);
            wscontainer.addEndpoint(wsClass);
            server.start();
            server.dump(System.err);
            server.join();
        } catch (Exception t) {
            t.printStackTrace(System.err);
        }
    }
}
