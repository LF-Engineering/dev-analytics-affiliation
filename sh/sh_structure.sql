-- MySQL dump 10.16  Distrib 10.1.47-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: lfinsights-db.cxuotfgkgdmj.us-west-2.rds.amazonaws.com    Database: lfinsights_prod
-- ------------------------------------------------------
-- Server version	10.4.8-MariaDB-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `countries`
--

DROP TABLE IF EXISTS `countries`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `countries` (
  `code` varchar(2) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `alpha3` varchar(3) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  PRIMARY KEY (`code`),
  UNIQUE KEY `_alpha_unique` (`alpha3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `domains_organizations`
--

DROP TABLE IF EXISTS `domains_organizations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `domains_organizations` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `domain` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `is_top_domain` tinyint(1) DEFAULT NULL,
  `organization_id` int(11) NOT NULL,
  `src` varchar(32) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `op` varchar(1) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `_domain_unique` (`domain`),
  KEY `organization_id` (`organization_id`),
  CONSTRAINT `domains_organizations_ibfk_1` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=7351 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `enrollments`
--

DROP TABLE IF EXISTS `enrollments`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `enrollments` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `start` datetime NOT NULL,
  `end` datetime NOT NULL,
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `organization_id` int(11) NOT NULL,
  `src` varchar(32) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `op` varchar(1) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `project_slug` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `role` varchar(20) COLLATE utf8mb4_unicode_520_ci NOT NULL DEFAULT 'Contributor',
  PRIMARY KEY (`id`),
  UNIQUE KEY `_period_unique` (`uuid`,`organization_id`,`start`,`end`,`project_slug`),
  KEY `organization_id` (`organization_id`),
  KEY `enrollments_project_slug_idx` (`project_slug`),
  KEY `enrollments_start_idx` (`start`),
  KEY `enrollments_end_idx` (`end`),
  KEY `enrollments_uuid_idx` (`uuid`),
  CONSTRAINT `enrollments_ibfk_1` FOREIGN KEY (`uuid`) REFERENCES `uidentities` (`uuid`) ON DELETE CASCADE,
  CONSTRAINT `enrollments_ibfk_2` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2604037 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `enrollments_archive`
--

DROP TABLE IF EXISTS `enrollments_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `enrollments_archive` (
  `archived_at` datetime(6) NOT NULL DEFAULT current_timestamp(6),
  `id` int(11) NOT NULL,
  `start` datetime NOT NULL,
  `end` datetime NOT NULL,
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `organization_id` int(11) NOT NULL,
  `project_slug` text COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `role` varchar(20) COLLATE utf8mb4_unicode_520_ci NOT NULL DEFAULT 'Contributor',
  KEY `enrollments_archive_archived_at_idx` (`archived_at`),
  KEY `enrollments_archive_id_idx` (`id`),
  KEY `enrollments_archive_uuid_idx` (`uuid`),
  KEY `enrollments_archive_organization_id_idx` (`organization_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `identities`
--

DROP TABLE IF EXISTS `identities`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `identities` (
  `id` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `name` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `email` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `username` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `source` varchar(32) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `last_modified` datetime(6) DEFAULT NULL,
  `src` varchar(32) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `op` varchar(1) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `_identity_unique` (`name`,`email`,`username`,`source`),
  KEY `uuid` (`uuid`),
  KEY `identity_email_idx` (`email`),
  KEY `identities_name_idx` (`name`),
  KEY `identities_username_idx` (`username`),
  KEY `identities_source_idx` (`source`),
  CONSTRAINT `identities_ibfk_1` FOREIGN KEY (`uuid`) REFERENCES `uidentities` (`uuid`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `identities_archive`
--

DROP TABLE IF EXISTS `identities_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `identities_archive` (
  `archived_at` datetime(6) NOT NULL DEFAULT current_timestamp(6),
  `id` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `name` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `email` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `username` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `source` varchar(32) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `last_modified` datetime(6) DEFAULT NULL,
  KEY `identities_archive_id_idx` (`id`),
  KEY `identities_archive_uuid_idx` (`uuid`),
  KEY `identities_archive_archived_at_idx` (`archived_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `matching_blacklist`
--

DROP TABLE IF EXISTS `matching_blacklist`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `matching_blacklist` (
  `excluded` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  PRIMARY KEY (`excluded`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `organizations`
--

DROP TABLE IF EXISTS `organizations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `organizations` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(191) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `src` varchar(32) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `op` varchar(1) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `_name_unique` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=61688 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `profiles`
--

DROP TABLE IF EXISTS `profiles`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `profiles` (
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `name` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `email` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `gender` varchar(32) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `gender_acc` int(11) DEFAULT NULL,
  `is_bot` tinyint(1) DEFAULT NULL,
  `country_code` varchar(2) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `src` varchar(32) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `op` varchar(1) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `country_code` (`country_code`),
  KEY `profiles_name_idx` (`name`),
  KEY `profiles_gender_idx` (`gender`),
  KEY `profiles_email_idx` (`email`),
  CONSTRAINT `profiles_ibfk_1` FOREIGN KEY (`uuid`) REFERENCES `uidentities` (`uuid`) ON DELETE CASCADE,
  CONSTRAINT `profiles_ibfk_2` FOREIGN KEY (`country_code`) REFERENCES `countries` (`code`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `profiles_archive`
--

DROP TABLE IF EXISTS `profiles_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `profiles_archive` (
  `archived_at` datetime(6) NOT NULL DEFAULT current_timestamp(6),
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `name` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `email` varchar(128) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `gender` varchar(32) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `gender_acc` int(11) DEFAULT NULL,
  `is_bot` tinyint(1) DEFAULT NULL,
  `country_code` varchar(2) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  KEY `profiles_archive_uuid_idx` (`uuid`),
  KEY `profiles_archive_archived_at_idx` (`archived_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `slug_mapping`
--

DROP TABLE IF EXISTS `slug_mapping`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `slug_mapping` (
  `da_name` varchar(128) NOT NULL,
  `sf_name` varchar(128) NOT NULL,
  `sf_id` varchar(64) NOT NULL,
  PRIMARY KEY (`da_name`,`sf_name`,`sf_id`),
  UNIQUE KEY `da_name` (`da_name`),
  UNIQUE KEY `sf_name` (`sf_name`),
  UNIQUE KEY `sf_id` (`sf_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `uidentities`
--

DROP TABLE IF EXISTS `uidentities`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `uidentities` (
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `last_modified` datetime(6) DEFAULT NULL,
  `src` varchar(32) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `op` varchar(1) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  PRIMARY KEY (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `uidentities_archive`
--

DROP TABLE IF EXISTS `uidentities_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `uidentities_archive` (
  `archived_at` datetime(6) NOT NULL DEFAULT current_timestamp(6),
  `uuid` varchar(128) COLLATE utf8mb4_unicode_520_ci NOT NULL,
  `last_modified` datetime(6) DEFAULT NULL,
  KEY `uidentities_archive_uuid_idx` (`uuid`),
  KEY `uidentities_archive_archived_at_idx` (`archived_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-12-03 10:15:01
