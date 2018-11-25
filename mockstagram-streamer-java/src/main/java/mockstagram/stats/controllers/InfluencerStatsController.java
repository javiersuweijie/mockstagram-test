package mockstagram.stats.controllers;

import mockstagram.stats.models.InfluencerStats;
import mockstagram.stats.streams.AggregateStatsByTimeWindow;
import mockstagram.stats.streams.LatestStats;
import mockstagram.stats.streams.RankInfluencers;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class InfluencerStatsController {

    KafkaStreams streams = null;

    @Autowired
    AggregateStatsByTimeWindow aggregateStatsByTimeWindow;

    @Autowired
    LatestStats latestStats;

    @Autowired
    RankInfluencers rankInfluencers;

    @RequestMapping(value = "/influencers/{pk}")
    public ResponseEntity<List<InfluencerStats>> getFollowersByPK(
            @PathVariable(value = "pk", required = true) Long pk) {
        return ResponseEntity.ok(aggregateStatsByTimeWindow.queryInfluencerStat(pk));
    }

    @RequestMapping(value = "/influencers/{pk}/latest")
    public ResponseEntity<InfluencerStats> getLatestStatsByPK(
            @PathVariable(value = "pk", required = true) Long pk) {
        return ResponseEntity.ok(latestStats.queryLatestStats(pk));
    }

    @RequestMapping(value = "/influencers/average")
    public ResponseEntity<Double> getLatestStatsByPK() {
        return ResponseEntity.ok(latestStats.queryAverageFollower());
    }

    @RequestMapping(value = "/influencers/{pk}/rank")
    public ResponseEntity<Long> getRankByPK(
            @PathVariable(value = "pk", required = true) Long pk) {

        return ResponseEntity.ok(rankInfluencers.queryRank(pk));
    }


}
