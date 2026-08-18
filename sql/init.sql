CREATE DATABASE IF NOT EXISTS ratelimit_gateway
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE ratelimit_gateway;

CREATE TABLE rate_limit_group (
    id          INT PRIMARY KEY AUTO_INCREMENT,
    group_name  VARCHAR(64)  NOT NULL UNIQUE COMMENT '分组名称，对应 NewAPI 的 token.group 字段',
    limit_5h    INT          NOT NULL DEFAULT 100    COMMENT '5小时配额',
    limit_7d    INT          NOT NULL DEFAULT 1000   COMMENT '7天配额',
    limit_30d   INT          NOT NULL DEFAULT 5000   COMMENT '30天配额',
    limit_type  ENUM('request', 'token') NOT NULL DEFAULT 'request' COMMENT '限速类型: 按请求数/按Token用量',
    scope       ENUM('key', 'group')    NOT NULL DEFAULT 'key'     COMMENT '限速粒度: 按Key独立/按分组共享',
    status      TINYINT      NOT NULL DEFAULT 1      COMMENT '1=启用 0=停用',
    remark      VARCHAR(256) DEFAULT NULL            COMMENT '备注',
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='分组限速策略配置';

INSERT INTO rate_limit_group (group_name, limit_5h, limit_7d, limit_30d, limit_type, scope, remark) VALUES
('default',    100,  1000,  5000,   'request', 'key',   '默认分组'),
('vip',        500,  5000,  50000,  'request', 'key',   'VIP分组'),
('enterprise', 2000, 20000, 200000, 'request', 'key',   '企业分组'),
('trial',      10,   50,    200,    'request', 'key',   '试用分组');
