;
-- Проверка связки ЮЛ-сотрудники;
CREATE OR REPLACE FUNCTION raise_exception(text)
RETURNS void AS $$
BEGIN
    RAISE EXCEPTION '%', $1;
END
$$ LANGUAGE plpgsql;

with validation as (SELECT CASE WHEN count(DISTINCT legal_entity_id) = 1 THEN 'ok'
								WHEN count(DISTINCT legal_entity_id) > 1 THEN 'err'
								END as is_ok
							FROM ekd_ekd.employee
							WHERE id IN ($ids_list))

SELECT raise_exception('В маршруте [$template_name] указаны сотрудники из разных ЮЛ.')
FROM validation
WHERE is_ok = 'err';

DROP FUNCTION IF EXISTS raise_exception(text)