# basedSearch
## 项目介绍
为简化用户对elasticsearch以及solr的操作，此项目对底层的各种操作进行了封装， 为用户提供统一的简洁的操作接口。
## 版本描述
- tag/5.0
  elasticsearch版本为：5.x
- tag/2.0
  elasticsearch版本为2.x


###  elastic的操作
### 5.x
  ```
  UpdateService updaeService = new EsUpdateService("127.0.0.1:9300","elastic","changeme","elasticsearch");
        String testId = "11111107";
        Map<String, String> updatedMap = Maps.newHashMap();
        updatedMap.put("age", "1920");
        updatedMap.put("height", "1921");
        updatedMap.put("nickname", "jasstion2");
        updatedMap.put("id", testId);
        updatedMap.put("userID", testId);

        updatedMap.put("type", "user");
        updatedMap.put("index", "demo_user");
        //UpdateService updaeService = new EsUpdateService();
        String registeDate="2012-03-10T09:23:12";
        updatedMap.put("registeDate",registeDate);

        //地理位置
        updatedMap.put("location","31.9886993504,116.4907671752");
        updatedMap.put("isSearchSourceData","1");
        updaeService.update(updatedMap);
        String testId = "11111107";
        Map<String, String> updatedMap = Maps.newHashMap();
        updatedMap.put("age", "1988");
        updatedMap.put("height", "1988");
        updatedMap.put("nickname", "jasstion1");
        updatedMap.put("id", testId);
        updatedMap.put("userID", testId);
        updatedMap.put("type", "user");
        updatedMap.put("index", "baihe_user");
        UpdateService updaeService = new EsUpdateService();
        String registeDate="2012-03-10T09:23:12";
        updatedMap.put("registeDate",registeDate);
        updaeService.add(updatedMap);
    ```

### 2.x
  ~~~~
  UpdateService updaeService = new EsUpdateService("http://120.131.7.145:9200/");
  String testId = "11111106";
       Map<String, String> updatedMap = Maps.newHashMap();
       updatedMap.put("age", "1920");
       updatedMap.put("height", "1920");
       updatedMap.put("nickname", "jasstion2");
       updatedMap.put("id", testId);
       updatedMap.put("userID", testId);

       updatedMap.put("type", "user");
       updatedMap.put("index", "baihe_user");
       UpdateService updaeService = new EsUpdateService();
       EsUpdateService("http://120.131.7.145:9200/");
       String registeDate="2012-03-10T09:23:12";
       updatedMap.put("registeDate",registeDate);

       //地理位置
       updatedMap.put("location","31.9886993504,116.4907671752");
       updatedMap.put("isSearchSourceData","1");
       updaeService.update(updatedMap);
       String testId = "11111107";
       Map<String, String> updatedMap = Maps.newHashMap();
       updatedMap.put("age", "1988");
       updatedMap.put("height", "1988");
       updatedMap.put("nickname", "jasstion1");
       updatedMap.put("id", testId);
       updatedMap.put("userID", testId);
       updatedMap.put("type", "user");
       updatedMap.put("index", "baihe_user");
       UpdateService updaeService = new EsUpdateService();
       String registeDate="2012-03-10T09:23:12";
       updatedMap.put("registeDate",registeDate);
       updaeService.add(updatedMap);
   ~~~~

  在对elasticsearch操作的时候，无论是update或者是add,map对象都必须以下值：
  index,type
  对于update还必须有id,
  而对于add如果没有id则表示自动生成id,如果有id则表示用此id作为主键。
  以上代码是针对单次保存和更新操作， 批量操作如下：
  ```
  List<Map<String, String>> maps = Lists.newArrayList();
  esUpdateService.bulkAdd(maps);
  ```



### solr的操作
目前solr仅仅支持4.6.1版本：
```

```
