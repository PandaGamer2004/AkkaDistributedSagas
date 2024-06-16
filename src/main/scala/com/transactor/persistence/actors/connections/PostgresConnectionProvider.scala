package com.transactor.persistence.actors.connections

import io.getquill._

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.postgresql.ds.PGSimpleDataSource;
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

object ConnectionConfiguration {
  def configurePostgresConnection(): PostgresJdbcContext[LowerCase.type] = {
    val postgresDataSource = new PGSimpleDataSource()
    postgresDataSource.setUser("postgres")
    postgresDataSource.setPassword("12345")

    val config = new HikariConfig()
    config.setDataSource(postgresDataSource)

    val hikariDataSource = new HikariDataSource(config)

    val resultContext = new PostgresJdbcContext(LowerCase, hikariDataSource)
    resultContext
  }

}
