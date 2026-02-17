
// Java Streams API - Log Generator v1.0
import java.io.*;
import java.util.Random;

/**
 * Generates a large server.log file for testing LogAnalyzer.
 * Creates ~10,000 log entries with realistic data.
 */
public class GenerateLog {

    static final String[] ENDPOINTS = {
            "/api/login", "/api/logout", "/api/dashboard", "/api/profile",
            "/api/settings", "/api/upload", "/api/download", "/api/search",
            "/api/users", "/api/reports", "/api/notifications", "/api/payments",
            "/api/orders", "/api/products", "/api/analytics"
    };

    static final int[] STATUS_CODES = { 200, 200, 200, 200, 200, 201, 301, 400, 401, 403, 404, 500, 502, 503 };

    public static void main(String[] args) throws IOException {
        int totalLines = 10_000;
        Random rand = new Random(42);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("server.log"))) {
            for (int i = 0; i < totalLines; i++) {
                String timestamp = String.format("2026-02-17T%02d:%02d:%02d",
                        rand.nextInt(24), rand.nextInt(60), rand.nextInt(60));
                String userId = "user" + (rand.nextInt(500) + 1);
                String endpoint = ENDPOINTS[rand.nextInt(ENDPOINTS.length)];
                // Slower endpoints for upload/download, faster for login
                long responseTime;
                if (endpoint.contains("upload") || endpoint.contains("download")) {
                    responseTime = 500 + rand.nextInt(3000);
                } else if (endpoint.contains("search") || endpoint.contains("analytics")) {
                    responseTime = 200 + rand.nextInt(1500);
                } else {
                    responseTime = 50 + rand.nextInt(500);
                }
                int statusCode = STATUS_CODES[rand.nextInt(STATUS_CODES.length)];

                writer.write(String.format("%s,%s,%s,%d,%d%n",
                        timestamp, userId, endpoint, responseTime, statusCode));
            }
        }

        System.out.println("Generated server.log with " + totalLines + " entries.");
    }
}
