package co.eitan101.examples;

import java.io.IOException;
import java.util.function.Consumer;
import javax.websocket.CloseReason;
import javax.websocket.EndpointConfig;
import javax.websocket.OnClose;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import org.codehaus.jackson.map.ObjectMapper;

@ServerEndpoint("/ws")
public class WsTest {
    
    @OnOpen
    public void open(final Session session, EndpointConfig conf) {
        ObjectMapper mapper = new ObjectMapper();
        Consumer handler = (Object event) -> {
            try {
                session.getAsyncRemote().sendText(mapper.writeValueAsString(event));
            } catch (IOException ex) {
            }
        };        
        session.getUserProperties().put("stream.listener",handler);
        PmQueryServerExample.gePmQueryServer().get("all").register(handler);        
    }

    @OnClose
    public void close(Session session, CloseReason reason) {
        System.out.println("closing "+reason);
        Consumer handler = (Consumer) session.getUserProperties().get("stream.listener");
        if (handler!=null)
            PmQueryServerExample.gePmQueryServer().get("all").unRegister(handler);
    }
}
