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
 * 非auto ack模式下接收消息的例子
 * 
 * @author boyan
 * 
 */
public class TransactionalConsumer {

    /** subscribe topic */
    public final static String topic = MetaqConfigConstant.TOPIC;

    /** consumer group */
    public final static String group = MetaqConfigConstant.GROUP;

    public static void main(final String[] args) throws Exception {
        // New session factory,强烈建议使用单例
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(initMetaConfig());
        // create consumer,强烈建议使用单例
        final MessageConsumer consumer = sessionFactory.createConsumer(new ConsumerConfig(group));

        consumer.subscribe(topic, 1024 * 1024, new MessageListener() {

            private int count = 0;

            @Override
            public void recieveMessages(final Message message) {
                System.out.println("Receive message " + new String(message.getData()));
                // set auto ack to false
                message.getPartition().setAutoAck(false);
                // ack once per two messages
                if (++this.count % 2 == 0) {
                    message.getPartition().ack();
                    System.out.println("ack message");
                }
            }
            @Override
            public Executor getExecutor() {
                // Thread pool to process messages,maybe null.
                return null;
            }
        });
        consumer.completeSubscribe();
    }
}