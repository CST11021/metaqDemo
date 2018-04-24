package com.taobao.metamorphosis.example.producer;

import static com.taobao.metamorphosis.example.Help.initMetaConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendMessageCallback;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.example.config.MetaqConfigConstant;


/**
 * 异步消息发送者
 *
 * @author 无花
 * @Date 2012-2-27
 */
public class AsyncProducer {

    public final static String topic = MetaqConfigConstant.TOPIC_SLAVE;

    public static void main(final String[] args) throws Exception {
        // New session factory,强烈建议使用单例
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(initMetaConfig());
        // create producer,强烈建议使用单例
        final MessageProducer producer = sessionFactory.createProducer();

        producer.publish(topic);

        String line;
        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        while ((line = readLine(reader)) != null) {
            try {
                producer.sendMessage(new Message(topic, line.getBytes()), new SendMessageCallback() {
                    @Override
                    public void onMessageSent(final SendResult result) {
                        if (result.isSuccess()) {
                            System.out.println("Send message successfully,sent to " + result.getPartition());
                        } else {
                            System.err.println("Send message failed,error message:" + result.getErrorMessage());
                        }

                    }

                    @Override
                    public void onException(final Throwable e) {
                        e.printStackTrace();
                    }
                });
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }


    private static String readLine(final BufferedReader reader) throws IOException {
        System.out.println("Type a message to send:");
        return reader.readLine();
    }
}
