package varys

import java.util.concurrent.TimeUnit._
import java.util.concurrent._

import com.google.common.util.concurrent.ThreadFactoryBuilder

/**
 * Various utility methods used by Varys.
 */
private object Utils extends Logging {

  private[varys] val daemonThreadFactory: ThreadFactory =
    new ThreadFactoryBuilder().setDaemon(true).build()

  /**
   * Runs the given code block periodically in daemon threads.
   * FIXME: Return a handle to stop this periodic task.
   */
  def scheduleDaemonAtFixedRate(initialDelay: Long,
                                period: Long,
                                threads: Int = 4)
                               (block: => Unit) {

    val task = new Runnable() {
      def run() {
        block
      }
    }
    val scheduler = Executors.newScheduledThreadPool(threads, daemonThreadFactory)
    scheduler.scheduleAtFixedRate(task, initialDelay, period, MILLISECONDS)
  }
}
