package ifuture;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private enum Compression {
        Compress,
        DeCompress
    }

    private static final String ParamMismatch = "Wrong parameters. Correct format: compress/decompress [source file] [target file]";
    private static volatile Long progress = 0L;


    public static void main(String[] args) {

        if (args.length < 3) {
            System.out.println(ParamMismatch);
            return;
        }

        var method = args[0];
        var source = args[1];
        var target = args[2];

        Compression compression;
        switch (method.toLowerCase()) {
            case "compress":
                compression = Compression.Compress;
                break;
            case "decompress":
                compression = Compression.DeCompress;
                break;
            default:
                System.out.println(ParamMismatch);
                return;
        }

        var processor = compression == Compression.Compress ? new Compressor() : new Decompressor();
        var progressExecutor = Executors.newSingleThreadScheduledExecutor();
        progressExecutor.scheduleAtFixedRate(
                () -> System.out.print("\r" + processor.getProgress() + " bytes"), 500, 500, TimeUnit.MILLISECONDS);

        var success = false;
        try {
            processor.process(source, target);
            success = true;
            System.out.println("\r" + compression.toString() + "ion completed");
        } catch (ProcessException e) {
            System.out.println(e.getMessage() + ": " + e.getCause().getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {
            progressExecutor.shutdownNow();
        }

        if (success)
            return;

        try {
            Files.deleteIfExists(Paths.get(target));
        } catch (IOException ex) {;}
    }

}
