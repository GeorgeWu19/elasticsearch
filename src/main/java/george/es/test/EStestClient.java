package george.es.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;

import java.io.IOException;

public class EStestClient {
    public static void main(String[] args) throws Exception {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1",9200,"http"))
        );

        System.out.println(client);

        // 创建索引
//        CreateIndexResponse user
//                = client.indices().create(new CreateIndexRequest("user_index"), RequestOptions.DEFAULT);
//        System.out.println(user.isAcknowledged() ? "操作成功" : "新增索引失败");

        // 查询索引
        GetIndexResponse response = client.indices().get(new GetIndexRequest("user_index"), RequestOptions.DEFAULT);
        System.out.println(response.getAliases());
        System.out.println(response.getMappings());
        System.out.println(response.getSettings());

        AcknowledgedResponse delRes = client.indices().delete(new DeleteIndexRequest("user_index"), RequestOptions.DEFAULT);
        System.out.println("删除成功？ " + delRes.isAcknowledged());

        client.close();
    }
}
