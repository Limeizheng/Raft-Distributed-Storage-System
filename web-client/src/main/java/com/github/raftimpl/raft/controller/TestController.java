package com.github.raftimpl.raft.controller;

import com.github.raftimpl.raft.template.RaftTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

//这是一个典型的Spring Boot控制器，通过HTTP协议对外暴露读写接口，属于对外提供的API。
@RestController
@RequestMapping("/raft")
public class TestController {
    @Autowired
    private RaftTemplate raftTemplate;

    @PostMapping("/write")
    public String write(@RequestParam String key, @RequestParam String value) {
        return raftTemplate.write(key, value);
    }

    @GetMapping("/read")
    public String read(@RequestParam String key) {
        return raftTemplate.read(key);
    }
}
