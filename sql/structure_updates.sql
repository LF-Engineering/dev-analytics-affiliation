DROP TABLE IF EXISTS `profiles_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `profiles_archive` (
  `archived_at` datetime(6) NOT NULL DEFAULT now(),
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `name` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `email` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `gender` varchar(32) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `gender_acc` int(11) DEFAULT NULL,
  `is_bot` tinyint(1) DEFAULT NULL,
  `country_code` varchar(2) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

DROP TABLE IF EXISTS `uidentities_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `uidentities_archive` (
  `archived_at` datetime(6) NOT NULL DEFAULT now(),
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `last_modified` datetime(6) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

DROP TABLE IF EXISTS `enrollments_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `enrollments_archive` (
  `archived_at` datetime(6) NOT NULL DEFAULT now(),
  `id` int(11) NOT NULL,
  `start` datetime NOT NULL,
  `end` datetime NOT NULL,
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `organization_id` int(11) NOT NULL
) ENGINE=InnoDB AUTO_INCREMENT=5347 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

DROP TABLE IF EXISTS `identities_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `identities_archive` (
  `archived_at` datetime(6) NOT NULL DEFAULT now(),
  `id` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `name` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `email` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `username` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `source` varchar(32) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `last_modified` datetime(6) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
