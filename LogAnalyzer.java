import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

public class LogAnalyzer {

    public static void main(String[] args) throws IOException {

        Path path = Paths.get("server.log");

        System.out.println("=".repeat(60));
        System.out.println("  REAL-TIME LOG ANALYZER — Streaming Approach");
        System.out.println("=".repeat(60));

        long startTime = System.nanoTime();
        long startMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        MetricsAccumulator metrics;

        try (Stream<String> lines = Files.lines(path)) {
            metrics = lines
                    .map(LogAnalyzer::parseLine)
                    .filter(Objects::nonNull)
                    .collect(
                            MetricsAccumulator::new,
                            MetricsAccumulator::accumulate,
                            MetricsAccumulator::combine
                    );
        }

        long endMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long streamingTime = System.nanoTime() - startTime;

        System.out.println("\n📊 Total log entries processed: " + metrics.totalEntries);
        System.out.println("⏱  Processing time: " + (streamingTime / 1_000_000) + " ms");
        System.out.println("💾 Approx memory used: " + ((endMemory - startMemory) / 1024) + " KB");

        System.out.println("\n" + "─".repeat(50));
        System.out.println("1️⃣  TOP 10 SLOWEST ENDPOINTS (avg response time)");
        System.out.println("─".repeat(50));

        Map<String, Double> avgResponseTime = metrics.endpointResponseTimes.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().stream()
                                .mapToLong(Long::longValue)
                                .average()
                                .orElse(0.0)
                ));

        avgResponseTime.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(10)
                .forEach(e -> System.out.printf("   %-25s → %8.2f ms%n", e.getKey(), e.getValue()));

        System.out.println("\n" + "─".repeat(50));
        System.out.println("2️⃣  ERROR RATE PER ENDPOINT");
        System.out.println("─".repeat(50));

        metrics.endpointTotalCount.forEach((endpoint, total) -> {
            long errors = metrics.endpointErrorCount.getOrDefault(endpoint, 0L);
            double rate = (errors * 100.0) / total;
            System.out.printf("   %-25s → %6.2f%% (%d/%d errors)%n", endpoint, rate, errors, total);
        });

        System.out.println("\n" + "─".repeat(50));
        System.out.println("3️⃣  P95 RESPONSE TIME");
        System.out.println("─".repeat(50));

        List<Long> sortedTimes = metrics.allResponseTimes.stream()
                .sorted()
                .collect(Collectors.toList());

        if (!sortedTimes.isEmpty()) {
            int p95Index = (int) Math.ceil(0.95 * sortedTimes.size()) - 1;
            long p95 = sortedTimes.get(Math.min(p95Index, sortedTimes.size() - 1));
            System.out.println("   P95 Response Time: " + p95 + " ms");
            System.out.println("   (95% of requests complete within " + p95 + " ms)");
        }

        System.out.println("\n" + "─".repeat(50));
        System.out.println("4️⃣  UNIQUE ACTIVE USERS");
        System.out.println("─".repeat(50));

        System.out.println("   Unique users: " + metrics.uniqueUsers.size());

        demonstrateLoadingApproach(path);

        System.out.println("\n" + "=".repeat(60));
        System.out.println("  Analysis Complete!");
        System.out.println("=".repeat(60));
    }

    static void demonstrateLoadingApproach(Path path) throws IOException {

        System.out.println("\n" + "─".repeat(50));
        System.out.println("📌 STREAMING vs LOADING COMPARISON");
        System.out.println("─".repeat(50));

        long loadStart = System.nanoTime();
        long loadMemBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        List<String> allLines = Files.readAllLines(path);
        List<LogEntry> allEntries = allLines.stream()
                .map(LogAnalyzer::parseLine)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        long loadMemAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long loadTime = System.nanoTime() - loadStart;

        System.out.println("\n   ❌ LOADING approach (Files.readAllLines):");
        System.out.println("      Time:   " + (loadTime / 1_000_000) + " ms");
        System.out.println("      Memory: ~" + ((loadMemAfter - loadMemBefore) / 1024) + " KB");
        System.out.println("      → Stores ALL " + allEntries.size() + " entries in RAM");
        System.out.println("      → Would FAIL on GB-scale files (OutOfMemoryError)");

        long streamStart = System.nanoTime();
        long streamMemBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        try (Stream<String> lineStream = Files.lines(path)) {
            long count = lineStream
                    .map(LogAnalyzer::parseLine)
                    .filter(Objects::nonNull)
                    .count();

            long streamMemAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long streamTime = System.nanoTime() - streamStart;

            System.out.println("\n   ✅ STREAMING approach (Files.lines):");
            System.out.println("      Time:   " + (streamTime / 1_000_000) + " ms");
            System.out.println("      Memory: ~" + ((streamMemAfter - streamMemBefore) / 1024) + " KB");
            System.out.println("      → Processed " + count + " entries with CONSTANT memory");
            System.out.println("      → Scales to GB-size files without issues");
        }
    }

    static LogEntry parseLine(String line) {
        try {
            String[] parts = line.split(",");
            if (parts.length < 5) return null;
            return new LogEntry(
                    parts[0].trim(),
                    parts[1].trim(),
                    parts[2].trim(),
                    Long.parseLong(parts[3].trim()),
                    Integer.parseInt(parts[4].trim())
            );
        } catch (Exception e) {
            return null;
        }
    }
}

class MetricsAccumulator {

    Map<String, List<Long>> endpointResponseTimes = new HashMap<>();
    Map<String, Long> endpointTotalCount = new HashMap<>();
    Map<String, Long> endpointErrorCount = new HashMap<>();
    List<Long> allResponseTimes = new ArrayList<>();
    Set<String> uniqueUsers = new HashSet<>();
    long totalEntries = 0;

    void accumulate(LogEntry entry) {
        totalEntries++;
        endpointResponseTimes
                .computeIfAbsent(entry.getEndpoint(), k -> new ArrayList<>())
                .add(entry.getResponseTime());
        endpointTotalCount.merge(entry.getEndpoint(), 1L, Long::sum);
        if (entry.getStatusCode() >= 400) {
            endpointErrorCount.merge(entry.getEndpoint(), 1L, Long::sum);
        }
        allResponseTimes.add(entry.getResponseTime());
        uniqueUsers.add(entry.getUserId());
    }

    void combine(MetricsAccumulator other) {
        other.endpointResponseTimes.forEach((k, v) ->
                endpointResponseTimes.merge(k, v, (a, b) -> { a.addAll(b); return a; }));
        other.endpointTotalCount.forEach((k, v) ->
                endpointTotalCount.merge(k, v, Long::sum));
        other.endpointErrorCount.forEach((k, v) ->
                endpointErrorCount.merge(k, v, Long::sum));
        allResponseTimes.addAll(other.allResponseTimes);
        uniqueUsers.addAll(other.uniqueUsers);
        totalEntries += other.totalEntries;
    }
}

class LogEntry {

    private final String timestamp;
    private final String userId;
    private final String endpoint;
    private final long responseTime;
    private final int statusCode;

    public LogEntry(String timestamp, String userId,
                    String endpoint, long responseTime,
                    int statusCode) {
        this.timestamp = timestamp;
        this.userId = userId;
        this.endpoint = endpoint;
        this.responseTime = responseTime;
        this.statusCode = statusCode;
    }

    public String getTimestamp() { return timestamp; }
    public String getUserId() { return userId; }
    public String getEndpoint() { return endpoint; }
    public long getResponseTime() { return responseTime; }
    public int getStatusCode() { return statusCode; }
}
