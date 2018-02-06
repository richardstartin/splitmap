package com.openkappa.splitmap;

import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.Option;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.profile.WinPerfAsmProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;

public class Launcher {

  public static void main(String[] args) throws RunnerException {
    ParsedArgs parsed = CliFactory.parseArguments(ParsedArgs.class, args);
    ChainedOptionsBuilder builder = new OptionsBuilder()
            .include(parsed.include())
            .mode(parsed.mode())
            .measurementTime(TimeValue.seconds(parsed.measurementTime()))
            .warmupIterations(10)
            .measurementIterations(10)
            .forks(1)
            .jvmArgsPrepend("-server")
            .shouldFailOnError(true)
            .verbosity(VerboseMode.EXTRA)
            .resultFormat(ResultFormatType.CSV);
    if (parsed.printAssembly()) {
      builder = builder.jvmArgsAppend("-XX:+UnlockDiagnosticVMOptions",
              "-XX:CompileCommand=print" + (null == parsed.methodName()
                      ? "" : ",*" + parsed.methodName()),
              "-XX:PrintAssemblyOptions=hsdis-print-bytes",
              "-XX:CompileCommand=print");
    }
    if (parsed.doPerfasm()) {
      String os = System.getProperty("os.name");
      if (os.toLowerCase().contains("windows")) {
        builder = builder.addProfiler(WinPerfAsmProfiler.class);
      }
      if (os.toLowerCase().contains("linux")) {
        builder = builder.addProfiler(LinuxPerfAsmProfiler.class);
      }
    }
    if (parsed.printCompilation()) {
      builder = builder.jvmArgsAppend("-XX:+PrintCompilation",
              "-XX:+UnlockDiagnosticVMOptions",
              "-XX:+PrintInlining");
    }
    if (null != parsed.output()) {
      builder = builder.output(parsed.output());
    }

    Runner runner = new Runner(builder.build());
    runner.list();
    runner.run();
  }

  public interface ParsedArgs {

    @Option(defaultValue = "com.openkappa.splitmap.*", shortName = "i", longName = "include")
    String include();

    @Option(shortName = "p", longName = "print-assembly")
    Boolean printAssembly();

    @Option(shortName = "n", longName = "method-name", defaultToNull = true)
    String methodName();

    @Option(shortName = "c", longName = "print-compilation")
    Boolean printCompilation();

    @Option(shortName = "x", longName = "perfasm")
    Boolean doPerfasm();

    @Option(defaultValue = "Throughput", shortName = "m", longName = "mode")
    Mode mode();

    @Option(defaultValue = "10", shortName = "t", longName = "time-seconds")
    int measurementTime();

    @Option(defaultToNull = true, shortName = "o", longName = "output")
    String output();
  }
}
