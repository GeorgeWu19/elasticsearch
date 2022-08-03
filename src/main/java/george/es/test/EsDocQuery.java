package george.es.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

public class EsDocQuery {
    public static void main(String[] args) throws Exception {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1",9200,"http"))
        );

        SearchRequest searchRequest = new SearchRequest();

        // 全量查询
        searchRequest.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));

        // 条件查询
        searchRequest.source(new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("name","张三")));

        // 分页查询 ,起始位置0 , 查询2条
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        sourceBuilder.from(0);
        sourceBuilder.size(2);
        searchRequest.source(sourceBuilder);

        // 查询排序
        SearchSourceBuilder sourceBuilder1 = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        sourceBuilder1.sort("name" , SortOrder.ASC);
        searchRequest.source(sourceBuilder);
        
        // 过滤字段 include-需展示 exlusive-需排除
        SearchSourceBuilder sourceBuilder2 = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        String[] include = {"name"};
        String[] exlusive = {};
        sourceBuilder2.fetchSource(include , exlusive);

        // 联合查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 必须满足该条件 .should() 相当于 or， .must() 相当于 and； 因此两者直接同时使用，只取must
        // 可以把shuould的连在一起，在外面再接一个must， 构成 A and (B or C)的效果
        boolQueryBuilder.must(QueryBuilders.termQuery("name","张三"));
        boolQueryBuilder.mustNot(QueryBuilders.termQuery("age","30"));
        searchRequest.source(new SearchSourceBuilder().query(boolQueryBuilder));

        // 范围查询 gte-大于等于 lt-小于
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age");
        rangeQueryBuilder.gte("30");
        rangeQueryBuilder.lt("40");
        searchRequest.source(new SearchSourceBuilder().query(rangeQueryBuilder));

        // 模糊查询  fuzziness(Fuzziness.ONE) 允许有一个字符的模糊，多一少一也可以
        FuzzyQueryBuilder fuzziness = QueryBuilders.fuzzyQuery("name", "张三").fuzziness(Fuzziness.ONE);
        searchRequest.source(new SearchSourceBuilder().query(fuzziness));

        // 高亮查询
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder allQueryBuilder = QueryBuilders.matchAllQuery();
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("name");

        searchSourceBuilder.query(allQueryBuilder);
        searchSourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(searchSourceBuilder);

        // 聚合查询-最大
        AggregationBuilder aggregationBuilder = AggregationBuilders.max("maxAge").field("age");
        searchRequest.source(new SearchSourceBuilder().aggregation(aggregationBuilder));
        // 聚合查询-分组 分组返回值会在返回的buckets中
        AggregationBuilder aggregationBuilder1 = AggregationBuilders.terms("ageGroup").field("age");
        searchRequest.source(new SearchSourceBuilder().aggregation(aggregationBuilder1));

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        System.out.println(hits.getTotalHits());
        for (SearchHit hit : hits) {
            System.out.println(hit.toString());
        }



        client.close();
    }
}
