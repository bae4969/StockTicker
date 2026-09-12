CREATE DATABASE IF NOT EXISTS `BithumbTest` CHARACTER SET utf8 COLLATE utf8_general_ci;
CREATE DATABASE IF NOT EXISTS `KoreaInvestTest` CHARACTER SET utf8 COLLATE utf8_general_ci;
CREATE DATABASE IF NOT EXISTS `tickTest` CHARACTER SET utf8 COLLATE utf8_general_ci;
CREATE DATABASE IF NOT EXISTS `candleTest` CHARACTER SET utf8 COLLATE utf8_general_ci;
CREATE DATABASE IF NOT EXISTS `LogTest` CHARACTER SET utf8 COLLATE utf8_general_ci;

GRANT ALL PRIVILEGES ON `BithumbTest`.* TO 'stockticker_test'@'%';
GRANT ALL PRIVILEGES ON `KoreaInvestTest`.* TO 'stockticker_test'@'%';
GRANT ALL PRIVILEGES ON `tickTest`.* TO 'stockticker_test'@'%';
GRANT ALL PRIVILEGES ON `candleTest`.* TO 'stockticker_test'@'%';
GRANT ALL PRIVILEGES ON `LogTest`.* TO 'stockticker_test'@'%';
