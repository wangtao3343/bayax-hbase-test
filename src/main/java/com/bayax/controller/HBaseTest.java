package com.bayax.controller;

import com.bayax.service.HBaseServiceImpl;
import org.apache.hadoop.hbase.Cell;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/hbase")
public class HBaseTest {

    @Autowired
    private HBaseServiceImpl hBaseService;

    /**
     *
     * @param tablename
     * @param families
     * @return
     */
    @GetMapping(value = "/create/{tablename}")
    public String create(@PathVariable("tablename") String tablename, @RequestParam("families") String families){
        String msg = "";
        if (!families.equals("")){
            String[] fs = families.split(",");
            msg = hBaseService.createTable(tablename, fs);
        }else{
            msg = hBaseService.createTable(tablename);
        }
        return msg;
    }

    /**
     *
     * @param tablename
     * @param regexKey
     * @return
     */
    @GetMapping(value="/scan/{tablename}")
    public String scan(@PathVariable("tablename") String tablename, @RequestParam("regexKey") String regexKey){

        long current = System.currentTimeMillis();

        hBaseService.scanRegexRowKey(tablename, regexKey);

        long end = System.currentTimeMillis();

        return (end - current) + " ms";
    }


    @GetMapping(value="/put/{tablename}")
    public String put(@PathVariable("tablename") String tablename,@RequestParam("key") String rowKey,
                    @RequestParam("f") String familyColumn,
                    @RequestParam("c") String columnNames,
                    @RequestParam("v") String  values){

        long current = System.currentTimeMillis();

        hBaseService.putRowValue(tablename, rowKey, familyColumn, columnNames, values);

        long end = System.currentTimeMillis();

        return (end - current) + " ms";

    }

}
