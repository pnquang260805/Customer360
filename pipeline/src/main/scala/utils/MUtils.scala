package utils

import com.typesafe.scalalogging.LazyLogging

trait MUtils extends LazyLogging{
    def benchmark[T](blockName : String)(f: => T): T = { // (blockName : String): tham số truyền vào, f: hàm truyền vào qua {}
      val t0 = System.nanoTime();
      val result = f; // Execute wrapped function
      val t1 = System.nanoTime();
      val duration = (t1 - t0) / 1000000.0;
      logger.info(s"Task [$blockName] executed in: ${"%.2f".format(duration)} ms")
      result;
    }
}
