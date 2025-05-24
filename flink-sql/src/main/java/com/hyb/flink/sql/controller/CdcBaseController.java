package com.hyb.flink.sql.controller;

import com.hyb.flink.sql.services.CdcBaseService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @program: flink-learn
 * @description:
 * @author: huyanbing
 * @create: 2025-05-24
 **/
@RestController
@RequestMapping("/datasource")
public class CdcBaseController {


    private CdcBaseService cdcBaseService;

    public CdcBaseController(CdcBaseService cdcBaseService) {
        this.cdcBaseService = cdcBaseService;
    }

    @PostMapping("/cdc/executeSql")
    public void getColumnMetadata(@RequestBody List<String> sqlList) {
        cdcBaseService.executeSql(sqlList);
    }

}
