# MSP mysql smart proxy
[![Build Status](https://travis-ci.org/compasses/mysql-smart-proxy.svg?branch=master)](https://travis-ci.org/compasses/mysql-smart-proxy)

## mysql smart proxy
### 简要介绍
1. 完全为项目需要，记录query，并分析slow query，也是为了进一步理解mysql协议。一方面也是受SaaS数据design的影响。
2. 作为中间件的原型，支持集群的水平扩展，分库分表等，支持基本的水平拆分功能。例如分schema存储等。
3. 后面未在项目中大规模使用，也未做更多的测试，使用benchmark做过两天的稳定测试。
4. 可根据ID区分不同的Server；连接池功能；记录slow query；简单的场景也可以用来分析页面sql 的使用频率等。

## 工作流程
![mysqlimage](./mysqlimage)

based on [mixer](https://github.com/siddontang/mixer) and [kingshard](https://github.com/flike/MSP)
