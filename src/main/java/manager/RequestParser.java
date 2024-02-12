package manager;

import org.json.JSONArray;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class RequestParser {
    public static Map<String, Review> parseRequest(InputStream inputFile) throws IOException {

        Map<String, Review> requestReviews = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JSONObject lineJson = new JSONObject(line);
                JSONArray reviewsJsons = lineJson.getJSONArray("reviews");
                for (Object reviewJson : reviewsJsons) {
                    Review review = Review.fromJson((JSONObject) reviewJson);
                    requestReviews.put(review.getId(), review);
                }
            }
        }

        return requestReviews;
    }

}
