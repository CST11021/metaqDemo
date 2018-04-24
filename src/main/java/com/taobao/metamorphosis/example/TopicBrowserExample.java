package com.taobao.metamorphosis.example;

import static com.taobao.metamorphosis.example.Help.initMetaConfig;

import java.util.Iterator;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.TopicBrowser;
import com.taobao.metamorphosis.example.config.MetaqConfigConstant;


public class TopicBrowserExample {

    public final static String topic = MetaqConfigConstant.TOPIC;

    public static void main(final String[] args) throws Exception {
        // New session factory,ǿ�ҽ���ʹ�õ���
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(initMetaConfig());

        final TopicBrowser browser = sessionFactory.createTopicBrowser(topic);

        Iterator<Message> it = browser.iterator();
        // �������topic�µ�������Ϣ
        while (it.hasNext()) {
            Message msg = it.next();
            System.out.println("message body:" + new String(msg.getData()));
        }

        browser.shutdown();
        sessionFactory.shutdown();
    }
}
