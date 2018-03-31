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
 * ��Ϣ������
 * 
 * @author boyan
 * @Date 2011-5-17
 *
    ��ʹ����Ϣ�����ߺ�������֮ǰ��������Ҫ�������ǣ������Ҫ�õ���Ϣ�Ự�����ࡪ��MessageSessionFactory���������������
���������߻��������ߡ�������Щ��MessageSessionFactory��ĬĬ���ŵ��ں���������ܶ����飬������
1.����Ĳ��Һͷ��֣�ͨ��diamond��zookeeper��������ճ���meta��������ַ�б�
2.���ӵĴ��������٣��Զ����������ٵ�meta�����������ӣ��������Ӹ��ã�Ҳ���ǵ�ͬһ̨meta�ķ�������һ��������ֻά��һ�����ӡ�
3.��Ϣ�����ߵ���Ϣ�洢�ͻָ����������ǻ�̸����һ�㡣
4.Э���͹��������Դ�����������������ߺ������ߵġ�
��ˣ�����������Ҫ����һ���Ự�����࣬MessageSessionFactory����һ���ӿڣ�����ʵ������MetaMessageSessionFactory��
��ע��,MessageSessionFactoryӦ���������ã�Ҳ������ΪӦ���еĵ�����ʹ�ã��򵥵������ǽ���spring֮������������йܡ�



    ��Meta�ÿ����Ϣ������Message���ʵ����Message��ʾһ����Ϣ������������ô�������ԣ�
id: Long�͵���Ϣid,��Ϣ��Ψһid��ϵͳ�Զ��������û��޷����ã��ڷ��ͳɹ����ɷ��������أ�����ʧ����Ϊ0��
topic: ��Ϣ�����⣬�����߶��ĸ����⼴�ɽ��շ��͵��������µ���Ϣ��������ͨ��ָ��������topic���ҵ���Ҫ���ӵķ�������ַ�����롣
data: ��Ϣ����Ч�غɣ����������ݣ�Ҳ������Ϣ���ݣ�meta��Զ�����޸���Ϣ���ݣ��㷢�ͳ�ȥ��ʲô���ӣ����յ�����ʲô���ӡ�
��Ϣ����ͨ��������1M���ڣ��ҵĽ�������ò�Ҫ���ͳ����ϰ�K����Ϣ�����롣�����Ƿ�ѹ��Ҳ��ȫȡ�����û���
attribute: ��Ϣ���ԣ�һ���ַ�������ѡ�������߿�������Ϣ�������������߹��ˡ�

ϸ�ĵ����ѿ���ע�⵽��������sendMessage֮ǰ��������MessageProducer��publish(topic)����
producer.publish(topic);
��һ���ڷ�����Ϣǰ�Ǳ���ģ�����뷢���㽫Ҫ������Ϣ��topic������Ϊ���ûỰ��������ȥ���ҽ�����Щtopic��meta��������ַ����ʼ�����ӡ�����������ÿ��topicֻ��Ҫ��һ�Σ���ε�����Ӱ�졣

�ܽ���������ӣ��ӱ�׼�����������������ݣ��������ݷ�װ��һ��Message���󣬷��͵�topicΪtest�ķ������ϡ�
��ע�⣬MessageProducer���̰߳�ȫ�ģ���ȫ���ظ�ʹ�ã���������Ӧ������Ϊ������ʹ�ã�һ�δ���������ʹ�ã�����Ϊspring���
singleton bean��MessageProducer�����Ĵ��۰���ÿ�ζ���Ҫͨ��zk���ҷ�����������tcp�����ӡ�


 */
public class Producer {
    public static void main(final String[] args) throws Exception {
        // New session factory,ǿ�ҽ���ʹ�õ���
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(initMetaConfig());
        // create producer,ǿ�ҽ���ʹ�õ���
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