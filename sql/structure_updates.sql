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
