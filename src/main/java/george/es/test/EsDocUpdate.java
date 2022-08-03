package george.es.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import george.es.test.dto.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class EsDocUpdate {
    public static void main(String[] args) throws Exception {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("127.0.0.1",9200,"http"))
        );

        /**
         * 新增文档
         */
        IndexRequest addRequest = new IndexRequest();
        addRequest.id("1001").index("user_index");

        User user = new User();
        user.setAge(21);
        user.setName("张三");
        user.setSex("男");
        // 需要将内容转为json
        ObjectMapper objectMapper = new ObjectMapper();
        String userJson = objectMapper.writeValueAsString(user);

//        addRequest.source(userJson , XContentType.JSON );
//        IndexResponse addRes = client.index(addRequest , RequestOptions.DEFAULT);
//        // 新增文档：CREATED
//        System.out.println("新增文档："+ addRes.getResult());

        /**
         * 修改文档
         */
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.id("1001").index("user_index");
        user.setAge(35);
        String userUpdateJson = objectMapper.writeValueAsString(user);
//        updateRequest.doc(userUpdateJson , XContentType.JSON );
//        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
//        // 修改文档：UPDATED
//        System.out.println("修改文档："+ updateResponse.getResult());

        /**
         * 查询文档
         */
        GetRequest getRequest = new GetRequest();
        getRequest.id("1001").index("user_index");
        GetResponse getRes = client.get(getRequest, RequestOptions.DEFAULT);
        // 查询文档：{"name":"张三","sex":"男","age":35}
        System.out.println("查询文档："+ getRes.getSourceAsString());

        /**
         * 删除文档
         */
        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.id("1001").index("user_index");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        // 删除文档：DELETED
        System.out.println("删除文档："+ deleteResponse.getResult());

        client.close();
    }
}
