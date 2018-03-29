package com.mocha.spark.util

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.commons.dbcp.BasicDataSource
import org.apache.log4j.Logger

object MySqlConnectionPool {
    val log = Logger.getLogger(MySqlConnectionPool.this.getClass)
    var ds: BasicDataSource = null

    def getDataSource = {
        if (ds == null) {
            ds = new BasicDataSource()
            ds.setUsername("root")
            ds.setPassword("123456")
            ds.setUrl("jdbc:mysql://192.168.2.190:3306/sparkStreaming")
            ds.setDriverClassName("com.mysql.jdbc.Driver")
            ds.setInitialSize(5)
            ds.setMaxActive(20)
            ds.setMinIdle(50)
            ds.setMaxIdle(100)
            ds.setMaxWait(5000)
            ds.setMinEvictableIdleTimeMillis(5 * 60 * 1000)
            ds.setTimeBetweenEvictionRunsMillis(10 * 60 * 1000)
            ds.setTestOnBorrow(true)
        }
        ds
    }

    def getConnection: Connection = {
        var connect: Connection = null
        try {
            if (ds != null) {
                connect = ds.getConnection
            } else {
                connect = getDataSource.getConnection
            }
        }
        connect
    }

    def shutDownDataSource: Unit = if (ds != null) {
        ds.close()
    }

    def closeConnection(rs: ResultSet, ps: PreparedStatement, connect: Connection): Unit = {
        if (rs != null) {
            rs.close
        }
        if (ps != null) {
            ps.close
        }
        if (connect != null) {
            connect.close
        }
    }

}