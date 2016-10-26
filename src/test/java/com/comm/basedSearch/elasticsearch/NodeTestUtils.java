package com.comm.basedSearch.elasticsearch;

import com.google.common.collect.Lists;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugin.javascript.JavaScriptPlugin;
import org.elasticsearch.plugins.Plugin;
import org.junit.After;
import org.junit.Before;
import org.xbib.elasticsearch.plugin.payload.PayloadPlugin;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by jasstion on 19/10/2016.
 */
public  class NodeTestUtils {
    public static final NodeTestUtils   nodeTestUtils=new NodeTestUtils();

    public static void main(String[] args) throws Exception{

              Node node=  nodeTestUtils.createNode();
        Client client=node.client();
        String indexName="com";
        String typeName="user";

        Settings indexSettings = Settings.settingsBuilder()
                .put("index.similarity.payload.type", "payload_similarity")
                .put("analysis.analyzer.payloads.type", "custom")
                .put("analysis.analyzer.payloads.tokenizer", "whitespace")
                .put("analysis.analyzer.payloads.filter.0", "lowercase")
                .put("analysis.analyzer.payloads.filter.1", "delimited_payload_filter")
                .build();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject(typeName)
                .startObject("properties")
                .startObject("field1")
                .field("type", "string")
                .field("analyzer", "payloads")
                .field("term_vector", "with_positions_offsets_payloads")
                .field("similarity", "payload") // resolves to org.elasticsearch.index.similarity.PayloadSimilarity
                .endObject()
                .startObject("field2")
                .field("type", "string")
                .endObject()
                .startObject("field3")
                .field("type", "integer")
                .endObject()

                .endObject()
                .endObject()
                .endObject().string();

        client.admin().indices()
                .prepareCreate(indexName)
                .setSettings(indexSettings)
                .addMapping(typeName, mapping)
                .execute().actionGet();

        client.prepareIndex(indexName, typeName, "1")
                .setSource(XContentFactory.jsonBuilder().startObject()
                                .field("des", "box|5.0 swimming")
                                .field("name","jack award")
                                .field("age",123)
                                .endObject()
                )
                .setRefresh(true)
                .execute().actionGet();
        nodeTestUtils.releaseNode(node);

    }

    private Node node;
    static {
        System.setProperty("path.home","/data/es/test");
    }

   public  Node createNode() {

        System.err.println("path.home = " + System.getProperty("path.home"));
        Settings nodeSettings = Settings.settingsBuilder()
                .put("path.home", System.getProperty("path.home"))
                .put("index.number_of_shards", 1)
                .put("index.number_of_replica", 0)
                .put("script.inline", true)
                .put("script.indexed", true)
                .build();
        Node node = new MockNode(nodeSettings, Lists.newArrayList(PayloadPlugin.class, JavaScriptPlugin.class));
        node.start();
        return node;
    }

    public void releaseNode(Node node) throws IOException {
        if (node != null) {
            node.close();
            deleteFiles();
        }
    }

    @Before
    public void setupNode() throws IOException {
        node = createNode();
    }

    @After
    public void cleanupNode() throws IOException {
        releaseNode(node);
    }

    private void deleteFiles() throws IOException {
        Path directory = Paths.get(System.getProperty("path.home") + "/data");
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}

class MockNode extends Node {

    MockNode(Settings settings, Class<? extends Plugin> classpathPlugin) {
        this(settings, list(classpathPlugin));
    }

    protected MockNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
        //super(settings);
        super(InternalSettingsPreparer.prepareEnvironment(settings, null), Version.CURRENT, classpathPlugins);
    }

    public MockNode(Settings settings) {
        this(settings, list());
    }

    private static Collection<Class<? extends Plugin>> list() {
        return new ArrayList();
    }

    private static Collection<Class<? extends Plugin>> list(Class<? extends Plugin> classpathPlugin) {
        Collection<Class<? extends Plugin>> list = new ArrayList();
        list.add(classpathPlugin);
        return list;
    }
}
