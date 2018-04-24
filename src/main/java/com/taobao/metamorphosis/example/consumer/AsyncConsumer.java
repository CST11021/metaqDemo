/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Authors:
 *   wuhua <wq163@163.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.metamorphosis.example.consumer;

import static com.taobao.metamorphosis.example.Help.initMetaConfig;

import java.util.concurrent.Executor;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.example.config.MetaqConfigConstant;


/**
 * �첽��Ϣ������
 * 
 * @author boyan
 * @Date 2011-5-17
 * 
 */
public class AsyncConsumer {

    /** subscribe topic */
    public final static String topic = MetaqConfigConstant.TOPIC;

    /** consumer group */
    public final static String group = MetaqConfigConstant.GROUP;

    public static void main(final String[] args) throws Exception {
        // New session factory,ǿ�ҽ���ʹ�õ���
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(initMetaConfig());

        // create consumer,ǿ�ҽ���ʹ�õ���
        ConsumerConfig consumerConfig = new ConsumerConfig(group);
        // Ĭ������ȡ�ӳ�Ϊ5�룬�������ó�100���룬�����ʵ��Ӧ��Ҫ�������á�
        consumerConfig.setMaxDelayFetchTimeInMills(100);

        final MessageConsumer consumer = sessionFactory.createConsumer(consumerConfig);
        consumer.subscribe(topic, 1024 * 1024, new MessageListener() {
            @Override
            public void recieveMessages(final Message message) {
                // if (true) {
                //     throw new RuntimeException("����ʧ�ܣ�message = " + message.toString());
                // }
                System.out.println("Receive message " + new String(message.getData()));
            }

            @Override
            public Executor getExecutor() {
                // Thread pool to process messages,maybe null.
                return null;
            }
        });
        // complete subscribe
        consumer.completeSubscribe();

    }

}