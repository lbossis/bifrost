package org.jboss.pnc.bifrost.endpoint.websocket;

import com.thetransactioncompany.jsonrpc2.JSONRPC2Error;
import com.thetransactioncompany.jsonrpc2.JSONRPC2ParseException;
import com.thetransactioncompany.jsonrpc2.JSONRPC2Request;
import io.prometheus.client.Counter;
import org.apache.commons.beanutils.BeanUtils;
import org.eclipse.microprofile.metrics.MetricUnits;
import org.eclipse.microprofile.metrics.annotation.Gauge;
import org.eclipse.microprofile.metrics.annotation.Timed;
import org.jboss.logging.Logger;
import org.jboss.pnc.api.bifrost.dto.Line;
import org.jboss.pnc.bifrost.common.scheduler.Subscriptions;

import javax.inject.Inject;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.RemoteEndpoint;
import javax.websocket.SendHandler;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
@ServerEndpoint("/socket")
@Timed
public class Socket {

    static final Counter exceptionsTotal = Counter.build()
            .name("Socket_Exceptions_Total")
            .help("Errors and Warnings counting metric")
            .labelNames("severity")
            .register();

    private Logger logger = Logger.getLogger(Socket.class);

    @Inject
    Subscriptions subscriptions;

    @Inject
    MethodFactory methodFactory;

    private SendHandler commandResponseHandler = result -> {
        if (!result.isOK()) {
            exceptionsTotal.labels("error").inc();
            logger.error("Error sending command response.", result.getException());
        }
    };

    private String LOGLINE_NOTIFICATION = "LOG";

    @OnOpen
    public void open(Session session) {
    }

    @OnClose
    public void close(Session session) {
        unsubscribeSession(session.getId());
    }

    @OnError
    public void onError(Session session, Throwable error) {
        exceptionsTotal.labels("error").inc();
        logger.error("Socket communication error.", error);
        unsubscribeSession(session.getId());
    }

    private void unsubscribeSession(String sessionId) {
        subscriptions.getAll()
                .stream()
                .filter(s -> s.getClientId().equals(sessionId))
                .forEach(s -> subscriptions.unsubscribe(s));
    }

    @OnMessage
    public void handleMessage(String message, Session session) {
        logger.debug("Received message: " + message);
        RemoteEndpoint.Async remote = session.getAsyncRemote();

        JSONRPC2Request request;
        try {
            request = JSONRPC2Request.parse(message);
        } catch (JSONRPC2ParseException e) {
            String err = "Cannot parse request.";
            exceptionsTotal.labels("error").inc();
            logger.error(err, e);
            sendErrorResult(remote, "undefined", new JSONRPC2Error(-11, err + e.getMessage(), null));
            return;
        }
        Object requestId = request.getID();

        Optional<Method<?>> maybeMethod = methodFactory.get(request.getMethod());
        if (!maybeMethod.isPresent()) {
            String err = "Unsupported method " + request.getMethod();
            exceptionsTotal.labels("warning").inc();
            logger.warn(err);
            sendErrorResult(remote, requestId, new JSONRPC2Error(-12, err, null));
            return;
        }
        Method method = maybeMethod.get();
        method.setSession(session);
        logger.info("Requested method: " + method.getName());

        Object methodParameter = null;
        try {
            methodParameter = method.getParameterType().getConstructor(new Class[] {}).newInstance(new Object[] {});
            BeanUtils.populate(methodParameter, request.getNamedParams());
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException e) {
            String err = "Cannot construct parameters for method: " + request.getMethod() + ".";
            exceptionsTotal.labels("error").inc();
            logger.error(err, e);
            sendErrorResult(remote, requestId, new JSONRPC2Error(-13, err + e.getMessage(), null));
            return;
        }

        Consumer<Line> lineConsumer = line -> sendLine(remote, line, requestId, session);

        Result result = method.apply(methodParameter, lineConsumer);
        logger.debug("Method invoked, result: " + result);
        sendResult(remote, requestId, result);
    }

    private void sendErrorResult(RemoteEndpoint.Async remote, Object requestId, JSONRPC2Error error) {
        JsonbJSONRPC2Response jsonrpc2Response = new JsonbJSONRPC2Response(error, requestId);

        sendResponse(remote, jsonrpc2Response);
    }

    private void sendResult(RemoteEndpoint.Async remote, Object requestId, Result result) {
        JsonbJSONRPC2Response jsonrpc2Response = new JsonbJSONRPC2Response(result, requestId);
        sendResponse(remote, jsonrpc2Response);
    }

    private void sendResponse(RemoteEndpoint.Async remote, JsonbJSONRPC2Response jsonrpc2Response) {
        String responseString = jsonrpc2Response.toJSONString();
        remote.sendText(responseString, commandResponseHandler);
        logger.debug("Text response sent: " + responseString);
    }

    private void sendLine(RemoteEndpoint.Async remote, Line line, Object requestId, Session session) {
        logger.trace("Sending line as text message: " + line.asString());
        JsonbJSONRPC2Response jsonrpc2Response = new JsonbJSONRPC2Response(new LineResult(line), requestId);
        remote.sendText(jsonrpc2Response.toJSONString(), lineResponseHandler(session));
    }

    private SendHandler lineResponseHandler(Session session) {
        return result -> {
            if (!result.isOK()) {
                exceptionsTotal.labels("error").inc();
                logger.error("Error sending log line.", result.getException());
                unsubscribeSession(session.getId());
            }
        };
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Gauge(name = "Socket_Err_Count", unit = MetricUnits.NONE, description = "Errors count")
    public int showCurrentErrCount() {
        return (int) exceptionsTotal.labels("error").get();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Gauge(name = "Socket_Warn_Count", unit = MetricUnits.NONE, description = "Warnings count")
    public int showCurrentWarnCount() {
        return (int) exceptionsTotal.labels("warning").get();
    }

}
