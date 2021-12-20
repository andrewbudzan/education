### Tips

1. Working in Jupyter notebook enable `spark.sql.repl.eagerEval.enabled` to render DataFrame without calling `show()`. <br>Number of rows to show can be controlled via `spark.sql.repl.eagerEval.maxNumRows` configuration.

```python
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
df
```
