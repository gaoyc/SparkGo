# 要点总结

1. 使用SQLContext创建的表必须是临时表，如要保存成非临时表. 必须使用HiveContext, 否则异常：  
`Tables created with SQLContext must be TEMPORARY. Use a HiveContext instead.`
