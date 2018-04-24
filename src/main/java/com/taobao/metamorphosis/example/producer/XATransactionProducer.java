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
package com.taobao.metamorphosis.example.producer;

import static com.taobao.metamorphosis.example.Help.initMetaConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.XADataSource;
import javax.transaction.TransactionManager;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.XAMessageSessionFactory;
import com.taobao.metamorphosis.client.XAMetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.XAMessageProducer;
import com.taobao.metamorphosis.example.config.MetaqConfigConstant;


/**
 * 发送者参与分布式事务的简单例子，基于atomikos
 *
 * @author boyan(boyan @ taobao.com)
 * @date 2011-8-26
 */
public class XATransactionProducer {

    public final static String topic = MetaqConfigConstant.TOPIC;

    public static void main(final String[] args) throws Exception {
        // 创建一个事务管理器
        final TransactionManager tm = new UserTransactionManager();

        final XAMessageSessionFactory xasf = getXAMessageSessionFactory();
        // create XA producer,it should be used as a singleton instance.
        XAMessageProducer xaMessageProducer = xasf.createXAProducer();
        // publish topic
        xaMessageProducer.publish(topic);
        // create XA datasource,reuse it.
        final XADataSource xads = getXADataSource();

        String line;
        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while ((line = readLine(reader)) != null) {
            final String address = line;
            try {
                final int uid = 100;

                // we should create a template every transaction.
                final XATransactionTemplate template = new XATransactionTemplate(tm, xads, xaMessageProducer);
                template.executeCallback(new XACallback() {
                    @Override
                    public Object execute(final Connection conn, final XAMessageProducer producer, Status status) throws Exception {
                        final PreparedStatement pstmt =
                                conn.prepareStatement("insert into orders(uid,address) values(?,?)");
                        pstmt.setInt(1, uid);
                        pstmt.setString(2, null);
                        if (pstmt.executeUpdate() <= 0) {
                            status.setRollbackOnly();
                            return null;
                        }
                        pstmt.close();
                        if (!producer.sendMessage(new Message(topic, address.getBytes())).isSuccess()) {
                            status.setRollbackOnly();
                        }
                        return null;
                    }
                });

            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建一个消息会话工厂
     *
     * @return
     * @throws Exception
     */
    private static XAMessageSessionFactory getXAMessageSessionFactory() throws Exception {
        return new XAMetaMessageSessionFactory(initMetaConfig());
    }

    /**
     * 获取数据源
     *
     * @return
     * @throws SQLException
     */
    private static XADataSource getXADataSource() throws SQLException {
        /**
         请在test库里创建orders表：
         CREATE TABLE `test`.`orders` (
         `id` int(11) NOT NULL AUTO_INCREMENT,
         `uid` int(11) NOT NULL,
         `address` varchar(255) NOT NULL,
         PRIMARY KEY (`id`)
         );
         */
        final MysqlXADataSource mysqlXADataSource = new MysqlXADataSource();
        mysqlXADataSource
                .setUrl("jdbc:mysql://localhost:3306/test?characterEncoding=utf8&connectTimeout=1000&autoReconnect=true");
        mysqlXADataSource.setUser("root");
        mysqlXADataSource.setPassword("123456");
        mysqlXADataSource.setPreparedStatementCacheSize(20);
        return mysqlXADataSource;
    }

    private static String readLine(final BufferedReader reader) throws IOException {
        System.out.println("Type message to send:");
        return reader.readLine();
    }

}