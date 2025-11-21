## Current todo
- Make sure not to run the program twice in a single day (idempotence)
- Manage double ids or null ids
- Delete all logs
- Replace inferSchema=True with a defined schema

## Main Steps

✅ Finaliser pom.xml (Spark + shade plugin).

❌ Créer SparkMain + la structure de packages.

❌ Implémenter DailyIntegrationJob (lecture CSV → diff → bal_diff + bal_latest).

❌ Implémenter ReportJob (aggrégats par département).

❌ Implémenter RecomputeDumpJob (reconstruction à partir de bal_diff).

❌ Implémenter DiffJob (comparaison de deux snapshots parquet).

❌ Écrire les 4 scripts .sh qui appellent spark-submit.

❌ Tester localement avec quelques petits CSV jouets, puis avec le script de test donné.