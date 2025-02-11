SELECT a.title,a.category_id,b.snippet_title 
FROM "db-yt-raw"."raw_statistics" a 
INNER JOIN "db-yt-raw"."clean_stat_ref_data" b 
ON a.category_id=CAST(b.id as int)
WHERE a.region='ca';

SELECT a.title,a.category_id,b.snippet_title 
FROM "db-yt-raw"."raw_statistics" a 
INNER JOIN "db-yt-raw"."clean_stat_ref_data" b 
ON a.category_id=b.id
WHERE a.region='ca';

---

SELECT a.title,a.category_id,b.snippet_title 
FROM "db-yt-clean"."raw_statistics" a 
INNER JOIN "db-yt-clean"."clean_stat_ref_data" b 
ON a.category_id=b.id
WHERE a.region='ca';

SELECT * 
FROM "db-yt-clean"."raw_statistics" a 
INNER JOIN "db-yt-clean"."clean_stat_ref_data" b 
ON a.category_id=b.id
WHERE a.region='ca';

---


