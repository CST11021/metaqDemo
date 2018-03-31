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
package com.taobao.metamorphosis.example;

import static com.taobao.metamorphosis.example.Help.initMetaConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;


/**
 * 消息发送者
 * 
 * @author boyan
 * @Date 2011-5-17
 *
    在使用消息生产者和消费者之前，我们需要创建它们，这就需要用到消息会话工厂类——MessageSessionFactory，由这个工厂帮你
创建生产者或者消费者。除了这些，MessageSessionFactory还默默无闻地在后面帮你做很多事情，包括：
1.服务的查找和发现，通过diamond和zookeeper帮你查找日常的meta服务器地址列表
2.连接的创建和销毁，自动创建和销毁到meta服务器的连接，并做连接复用，也就是到同一台meta的服务器在一个工厂内只维持一个连接。
3.消息消费者的消息存储和恢复，后续我们会谈到这一点。
4.协调和管理各种资源，包括创建的生产者和消费者的。
因此，我们首先需要创建一个会话工厂类，MessageSessionFactory仅是一个接口，它的实现类是MetaMessageSessionFactory：
请注意,MessageSessionFactory应当尽量复用，也就是作为应用中的单例来使用，简单的做法是交给spring之类的容器帮你托管。



    在Meta里，每个消息对象都是Message类的实例，Message表示一个消息对象，它包含这么几个属性：
id: Long型的消息id,消息的唯一id，系统自动产生，用户无法设置，在发送成功后由服务器返回，发送失败则为0。
topic: 消息的主题，订阅者订阅该主题即可接收发送到该主题下的消息，生产者通过指定发布的topic查找到需要连接的服务器地址，必须。
data: 消息的有效载荷，二进制数据，也就是消息内容，meta永远不会修改消息内容，你发送出去是什么样子，接收到就是什么样子。
消息内容通常限制在1M以内，我的建议是最好不要发送超过上百K的消息，必须。数据是否压缩也完全取决于用户。
attribute: 消息属性，一个字符串，可选。发送者可设置消息属性来让消费者过滤。

细心的朋友可能注意到，我们在sendMessage之前还调用了MessageProducer的publish(topic)方法
producer.publish(topic);
这一步在发送消息前是必须的，你必须发布你将要发送消息的topic，这是为了让会话工厂帮你去查找接收这些topic的meta服务器地址并初始化连接。这个步骤针对每个topic只需要做一次，多次调用无影响。

总结下这个例子，从标准输入读入你输入的数据，并将数据封装成一个Message对象，发送到topic为test的服务器上。
请注意，MessageProducer是线程安全的，完全可重复使用，因此最好在应用中作为单例来使用，一次创建，到处使用，配置为spring里的
singleton bean。MessageProducer创建的代价昂贵，每次都需要通过zk查找服务器并创建tcp长连接。


 */
public class Producer {
    public static void main(final String[] args) throws Exception {
        // New session factory,强烈建议使用单例
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(initMetaConfig());
        // create producer,强烈建议使用单例
        final MessageProducer producer = sessionFactory.createProducer();
        // publish topic
        final String topic = "meta-test";
        producer.publish(topic);

        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;
        while ((line = readLine(reader)) != null) {
            // send message
            final SendResult sendResult = producer.sendMessage(new Message(topic, line.getBytes()));
            if (!sendResult.isSuccess()) {
                System.err.println("Send message failed,error message:" + sendResult.getErrorMessage());
            }
            else {
                System.out.println("Send message successfully,sent to " + sendResult.getPartition());
            }
        }
    }

    private static String readLine(final BufferedReader reader) throws IOException {
        System.out.println("Type a message to send:");
        return reader.readLine();
    }
}