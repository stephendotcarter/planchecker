-- Main table to store plans
CREATE TABLE plans (
    id          INT NOT NULL AUTO_INCREMENT,
    ref         VARCHAR(16) NOT NULL,
    plantext    TEXT NOT NULL,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- Add unique index on Ref field
ALTER TABLE `plans` ADD UNIQUE INDEX `unique_index_ref` (`ref`);
