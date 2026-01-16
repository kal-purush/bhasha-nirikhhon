package com.xiaojihua;

import org.apache.http.HttpServerConnection;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import java.io.IOException;

public class SolrJTest {

    /**
     * 测试添加文档索引
     * @throws IOException
     * @throws SolrServerException
     */
    @Test
    public void test() throws IOException, SolrServerException {
        // 和solr服务器创建连接
        // 参数：solr服务器的地址
        // 如果solr只有一个core那么下面的url中只写到solr就行，默认往第一个core中增加
        // 如果有多个core要像现在这样写，写那个core就是往哪个core中增加
        HttpSolrServer solrServer = new HttpSolrServer("http://localhost:8080/solr/collection1");
        // 创建一个文档对象
        SolrInputDocument document = new SolrInputDocument();
        // 向文档中添加域
        // 第一个参数：域的名称，域的名称必须是在schema.xml中定义的
        // 第二个参数：域的值
        document.addField("id","c0002");
        document.addField("name","solrj添加name2");
        document.addField("content","solrj添加的content2");
        // 把document对象添加到索引库中
        solrServer.add(document);
        // 提交修改
        solrServer.commit();

    }

    /**
     * 测试根据id删除文档
     * @throws IOException
     * @throws SolrServerException
     */
    @Test
    public void test2() throws IOException, SolrServerException {
        HttpSolrServer solrServer = new HttpSolrServer("http://localhost:8080/solr/collection1");
        solrServer.deleteById("c0002");
        solrServer.commit();
    }

    /**
     * 测试根据查询语句来删除
     * @throws IOException
     * @throws SolrServerException
     */
    @Test
    public void test3() throws IOException, SolrServerException {
        HttpSolrServer solrServer = new HttpSolrServer("http://localhost:8080/solr/collection1");
        solrServer.deleteByQuery("*:*");
        solrServer.commit();
    }


    @Test
    public void test4() throws SolrServerException {
        // 创建与solr服务器的链接对象
        HttpSolrServer solrServer = new HttpSolrServer("http://localhost:8080/solr/collection1");
        // 创建一个query对象
        SolrQuery query = new SolrQuery();
        // 设置查询条件
        query.setQuery("*:*");
        // 执行查询
        QueryResponse response = solrServer.query(query);
        // 获取查询结果
        SolrDocumentList results = response.getResults();
        // 共查询到的商品总数
        System.out.println("共查询到的商品总数：" + results.getNumFound());
        // 遍历查询结果
        for(SolrDocument doc : results){
            System.out.println(doc.get("id"));
            System.out.println(doc.get("name"));
            System.out.println(doc.get("content"));

        }
    }
}