package br.com.wendelsegadilha;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> oderDispatcher = new KafkaDispatcher<Order>();
    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<String>();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
            try {
                var email = req.getParameter("email");
                var amount = new BigDecimal(req.getParameter("amount"));
                var oderId = UUID.randomUUID().toString();

                var order = new Order(oderId, amount, email);
                oderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);
                var emailContent = "Pedido de compra em procesamento";
                emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailContent);

                System.out.println("New order sent successfully");
                resp.setStatus(HttpServletResponse.SC_OK);
                resp.getWriter().println("<h1>New order sent successfully</h1>");

                System.out.println(oderDispatcher);
                System.out.println(emailDispatcher);

            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
    }

    @Override
    public void destroy() {
        super.destroy();
        oderDispatcher.close();
        emailDispatcher.close();
    }

}
