package org.angnysa.dispatcher;

import lombok.SneakyThrows;
import lombok.Value;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MainTest {

    private static final int DURATION = 60_000;
    private static final int THREADS = 20;
    private static final int GROUPS = 100;

    @Value
    static class Message {
        private final byte[] data = new byte[4*1024*1024];
        {
            Arrays.fill(data, (byte) 0);
        }
        private final long id;
        private final long group;
    }

    static class StdoutStatMonitor<M> implements Monitor<M> {
        private AtomicLong counter = new AtomicLong();

        @Override
        public void onMessageStarting(M message) {}

        @Override
        public void onMessageComplete(M message) {
            counter.incrementAndGet();
        }

        @Override
        public void onMessageFailure(M message, Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        System.out.println("Warm up ...");
        test(1, 4, 4);

        System.out.print(",");
        for (int g = 1; g <= GROUPS; g++) {
            System.out.print(String.format("Groups: %d,", g));
        }
        System.out.println();

        for (int t=1; t<=THREADS; t++) {
            System.out.print(String.format("Threads: %d,", t));

            for (int g = 1; g <= GROUPS; g++) {
                System.out.print(String.format("%d,", test(DURATION, t, g)));
            }

            System.out.println();
        }
    }

    private static long test(long duration, int threads, int groups) throws InterruptedException {
        StdoutStatMonitor<Message> ssm = new StdoutStatMonitor<>();
        Dispatcher<Message, Long> d = new Dispatcher<>(0, threads, 1, TimeUnit.SECONDS,
                Message::getGroup
                , MainTest::process
                , ssm);

        Message[] mesgs = new Message[groups];
        for (int i=0; i<groups; i++) {
            mesgs[i] = new Message(i, i%groups);
        }

        long end = System.currentTimeMillis() + duration;
        int i = 0;
        while (System.currentTimeMillis() <= end) {
            d.submit(mesgs[i%groups]);
            i++;
        }
        d.shutdown();
        return ssm.counter.get();
    }

    @SneakyThrows(IOException.class)
    private static void process(Message m) {
        FileOutputStream fos = new FileOutputStream(new File("/tmp", "testdispatcher-"+Long.toString(m.group)));
        fos.write(m.data);
        fos.close();
    }
}
