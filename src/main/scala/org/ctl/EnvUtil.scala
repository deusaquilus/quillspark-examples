package org.ctl

object EnvUtil {
  def envVarOrError(envVarKey:String): String =
    Option(System.getenv(envVarKey))
      .getOrElse(throw new IllegalArgumentException(
        s"Could not find required parameter ${envVarKey} from environment vars. Need to specify one.")
      )
}
