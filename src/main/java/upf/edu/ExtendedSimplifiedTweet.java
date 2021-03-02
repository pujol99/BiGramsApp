package upf.edu;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Optional;

public class ExtendedSimplifiedTweet {

    private static JsonParser parser = new JsonParser();

    private final Long tweetId;              // the id of the tweet ('id')
    private final String text;              // the content of the tweet ('text')
    private final Long userId;              // the user id ('user->id')
    private final String userName;          // the user name ('user'->'name')
    private final String language;          // the language of a tweet ('lang')
    private final Long followersCount;      // the number of followers (’user’->’followers_count’)
    private final Boolean isRetweeted;      // is it a retweet? (the object ’retweeted_status’ exists?)
    private final Long retweetedUserId; // [if retweeted] (’retweeted_status’->’user’->’id’)
    private final Long retweetedTweetId; // [if retweeted] (’retweeted_status’->’id’)
    private final String retweetedUserName;
    private final Long timestampMs;          // seconds from epoch ('timestamp_ms')
    private final String retweetedText;

    public ExtendedSimplifiedTweet(Long tweetId, String text, Long userId, String userName,
                                   Long followersCount, String language, Boolean isRetweeted,
                                   Long retweetedUserId, Long retweetedTweetId, String retweetedUserName, String retweetedText, Long timestampMs) {
        this.tweetId = tweetId;
        this.text = text;
        this.userId = userId;
        this.userName = userName;
        this.language = language;
        this.followersCount = followersCount;         //new parameter
        this.isRetweeted = isRetweeted;               //new parameter
        this.retweetedUserId = retweetedUserId;       //new parameter
        this.retweetedTweetId = retweetedTweetId;     //new parameter
        this.retweetedUserName = retweetedUserName;   //new parameter
        this.timestampMs = timestampMs;
        this.retweetedText = retweetedText;
    }

    /**
     * Returns a {@link upf.edu.ExtendedSimplifiedTweet} from a JSON String.
     * If parsing fails, for any reason, return an {@link Optional#empty()}
     *
     * @param jsonStr
     * @return an {@link Optional} of a {@link upf.edu.ExtendedSimplifiedTweet}
     */
    public static Optional<upf.edu.ExtendedSimplifiedTweet> fromJson(String jsonStr) {
        Long tweetId = null, userId = null, timestampMs = null, followersCount = null, retweetedUserId = null, retweetedTweetId = null;
        String text = null, userName = null, language = null, retweetedUserName = null, retweetedText = null;
        Boolean isRetweeted = null;

        JsonElement json_aux = upf.edu.ExtendedSimplifiedTweet.parser.parse(jsonStr);
        JsonObject final_tweet = json_aux.getAsJsonObject();
        //System.out.println(final_tweet);

        try {
            Optional<Long> oTweetId = Optional.ofNullable(final_tweet.get("id").getAsLong());
            //System.out.println("TweetId: " + oTweetId);
            if (oTweetId.isPresent()) {
                tweetId = final_tweet.get("id").getAsLong();
            } else
                return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }


        Optional<String> oText = Optional.ofNullable(final_tweet.get("text").getAsString());
        if (oText.isPresent()) {
            text = final_tweet.get("text").getAsString();
        } else
            return Optional.empty();

        Optional<String> oLanguage = Optional.ofNullable(final_tweet.get("lang").getAsString().replace("\n", " "));
        if (oLanguage.isPresent()) {
            language = final_tweet.get("lang").getAsString().replace("\n", " ");
        } else
            return Optional.empty();

        Optional<Long> oTimestampMs = Optional.ofNullable(final_tweet.get("timestamp_ms").getAsLong());
        if (oTimestampMs.isPresent()) {
            timestampMs = final_tweet.get("timestamp_ms").getAsLong();
        } else
            return Optional.empty();

        Optional<JsonObject> oUser = Optional.ofNullable(final_tweet.get("user").getAsJsonObject());
        if (oUser.isPresent()) {
            JsonObject user = final_tweet.get("user").getAsJsonObject();
            Optional<Long> oUserId = Optional.ofNullable(user.get("id").getAsLong());
            if (oUserId.isPresent()) {
                userId = user.get("id").getAsLong();
            } else {
                return Optional.empty();
            }
            Optional<String> oUserName = Optional.ofNullable(user.get("name").getAsString());
            if (oUserName.isPresent()) {
                userName = user.get("name").getAsString();
            } else {
                return Optional.empty();
            }
            Optional<Long> oFollowCount = Optional.ofNullable(user.get("followers_count").getAsLong());
            if (oFollowCount.isPresent()) {
                followersCount = user.get("followers_count").getAsLong();
            }

        } else
            return Optional.empty();

        try {
            Optional<JsonObject> oIsRetweeted = Optional.ofNullable(final_tweet.get("retweeted_status").getAsJsonObject());
            if (oIsRetweeted.isPresent()) {
                isRetweeted = true;
                JsonObject retweetedInfo = final_tweet.get("retweeted_status").getAsJsonObject();

                Optional<Long> oRetweetedTweetId = Optional.ofNullable(retweetedInfo.get("id").getAsLong());
                if (oRetweetedTweetId.isPresent()) {
                    retweetedTweetId = retweetedInfo.get("id").getAsLong();
                }

                Optional<String> oRetweetedText = Optional.ofNullable(retweetedInfo.get("text").getAsString());
                if (oRetweetedText.isPresent()) {
                    retweetedText = retweetedInfo.get("text").getAsString();
                }

                Optional<JsonObject> oRetweetedUser = Optional.ofNullable(retweetedInfo.get("user").getAsJsonObject());
                if (oRetweetedUser.isPresent()) {
                    JsonObject retweetedUser = retweetedInfo.get("user").getAsJsonObject();
                    Optional<Long> oUserId = Optional.ofNullable(retweetedUser.get("id").getAsLong());
                    if (oUserId.isPresent()) {
                        retweetedUserId = retweetedUser.get("id").getAsLong();
                    }
                    Optional<String> oUserName = Optional.ofNullable(retweetedUser.get("name").getAsString());
                    if (oUserName.isPresent()) {
                        retweetedUserName = retweetedUser.get("name").getAsString();
                    }

                }
            }

        } catch (Exception e) {
            isRetweeted = false;
        }


        upf.edu.ExtendedSimplifiedTweet simplified_tweet = new upf.edu.ExtendedSimplifiedTweet(tweetId, text, userId, userName,
                followersCount, language, isRetweeted,
                retweetedUserId, retweetedTweetId, retweetedUserName, retweetedText, timestampMs);

        return Optional.ofNullable(simplified_tweet);


    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public long getTweetId() {
        return tweetId;
    }

    public String getText() {
        return text;
    }

    public long getUserId() {
        return userId;
    }

    public String getUserName() {
        return userName;
    }

    public String getLanguage() {
        return language;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public Long getFollowersCount() {
        return followersCount;
    }

    public Boolean getIsRetweeted() {
        return isRetweeted;
    }

    public Long getRetweetedUserId() {
        return retweetedUserId;
    }

    public Long getRetweetedTweetId() {
        return retweetedTweetId;
    }

    public String getRetweetedUserName() {
        return retweetedUserName;
    }

    public String getRetweetedText() {
        return retweetedText;
    }
}
