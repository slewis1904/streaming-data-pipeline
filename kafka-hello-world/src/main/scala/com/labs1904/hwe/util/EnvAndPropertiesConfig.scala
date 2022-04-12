package com.labs1904.hwe.util

import org.apache.commons.configuration2._
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
/*
import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }
// this can be set into the JVM environment variables, you can easily find it on google
val configPath = System.getProperty("config.path")
val config = ConfigFactory.parseFile(new File(configPath + "myFile.conf"))
config.getString("username")
*/

object EnvAndPropertiesConfig {
  def nullOrBlank(s: String): Boolean = {
    s == null || s.isEmpty
  }
  def getEnvKey(key: String): String = {
    key.toUpperCase.replace(".","-")
  }
  def getPropertiesKey(key: String): String = {
    key.toUpperCase.replace("_",".")
  }
}

class EnvAndPropertiesConfig(configFilename: String) {
  val envVarConfig = new EnvironmentConfiguration()
  val fileConfig: FileBasedConfiguration = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
    .configure(new Parameters().properties()
    .setFileName(configFilename)
    ).getConfiguration

  def getRequired () = {

  }
}