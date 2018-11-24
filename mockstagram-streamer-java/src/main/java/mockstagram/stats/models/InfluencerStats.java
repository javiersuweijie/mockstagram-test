package mockstagram.stats.models;

import java.sql.Timestamp;
import java.time.Instant;

public class InfluencerStats {
    private Long pk;
    private String username;
    private Long followerCount;
    private Long followingCount;

    public Long getPk() {
        return pk;
    }

    public void setPk(Long pk) {
        this.pk = pk;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Long getFollowerCount() {
        return followerCount;
    }

    public void setFollowerCount(Long followerCount) {
        this.followerCount = followerCount;
    }

    public Long getFollowingCount() {
        return followingCount;
    }

    public void setFollowingCount(Long followingCount) {
        this.followingCount = followingCount;
    }

    @Override
    public String toString() {
        return "InfluencerStats{" +
                "pk=" + pk +
                ", username='" + username + '\'' +
                ", followerCount=" + followerCount +
                ", followingCount=" + followingCount +
                '}';
    }
}
