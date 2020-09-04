package self.spark.scala.demo;

import org.junit.Test;
import self.spark.java.demo.Demo;
import self.spark.java.demo.SampleApp;

/**
 * ScalaTest
 *
 * @author chenzb
 * @date 2020/8/15
 */
public class ScalaTest {

	@Test
	public void test() {
		Demo demo = new Demo();
		demo.demo();
	}
}
