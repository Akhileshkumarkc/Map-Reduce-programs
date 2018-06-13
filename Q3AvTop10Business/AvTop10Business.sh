hdfs dfs -rm -r AvTop10Business
hdfs dfs -rm -r intermediate.csv
hadoop jar /home/shalin/IdeaProjects/WordCount1/out/artifacts/AvTop10Business_jar/AvTop10Business.jar /user/shalin/review.csv /user/shalin/business.csv user/shalin/intermediate.csv userAvTop10Business

