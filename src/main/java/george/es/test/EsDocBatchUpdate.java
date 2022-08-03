package george.es.test;

import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;

public class EsDocBatchUpdate {
    public static void main(String[] args) throws Exception {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1",9200,"http"))
        );

        // 批量请求
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest().index("user_index").id("1001").source(XContentType.JSON ,"name", "张三"));
        bulkRequest.add(new IndexRequest().index("user_index").id("1002").source(XContentType.JSON ,"name", "李四"));
        bulkRequest.add(new IndexRequest().index("user_index").id("1003").source(XContentType.JSON ,"name", "王五"));

        BulkResponse bulkRes = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        // 276ms
        System.out.println(bulkRes.getTook());
        System.out.println(Arrays.toString(bulkRes.getItems()));


        client.close();
    }
}
