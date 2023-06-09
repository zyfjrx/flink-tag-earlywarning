import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/6/9 13:24
 **/
public class MailTest {
    public static void main(String[] args) throws MessagingException, UnsupportedEncodingException {


        String smtpServer = "smtp.126.com";
        String username = "zhangyf_coder@126.com";
        //这里是你开通smtp协议的授权码，若是公司自定义服务器，可无需授权码，但需要配置证书，文章后面有详解
        String password = "NQUXXMWQNSRKPRZK";
        String receiver = "zhangyfcoder@163.com";
        String receiver2 = "zhangyf1130@outlook.com";

        //这里的配置可以自己抽取成工具
        Properties properties = new Properties();
        Map<String, Object> map = new HashMap<>();

        //常用smtp使用配置，可以在其他文章中获取：这里针对使用qq发送邮件
        map.put("mail.transport.protocol", "smtp");
        map.put("mail.smtp.host", smtpServer);
        map.put("mail.smtp.auth", "true");
        map.put("mail.smtp.port", "465");
        map.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        map.put("mail.smtp.socketFactory.fallback", false);
        map.put("mail.smtp.socketFactory.port", "465");
        map.put("mail.smtp.starttls.enable", "true");
        properties.putAll(map);

        //创建会话对象，用户邮件和服务器的交互
        Session session = Session.getDefaultInstance(properties);
//        session.setDebug(true); //查看发送邮件的log

        //创建一邮件
        MimeMessage message = new MimeMessage(session);
        InternetAddress senderAddress = new InternetAddress(username, "zyf", "UTF-8");
        message.setFrom(senderAddress);
        message.setRecipient(Message.RecipientType.TO, new InternetAddress(receiver, receiver, "UTF-8"));

        message.setSubject("测试", "UTF-8");
        message.setContent("flink告警测试", "text/html;charset=UTF-8");
        message.setSentDate(new Date());
        message.saveChanges();

        //用session 获取传输对象,然后连接发件人
        Transport transport = session.getTransport();
        transport.connect(username, password);
        transport.sendMessage(message, message.getAllRecipients());
        transport.close();
        System.out.println("发送成功");
    }
}