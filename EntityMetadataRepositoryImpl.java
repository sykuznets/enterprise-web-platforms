package com.example.project.repository.metadata;

import com.example.project.model.EntityMetadata;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class EntityMetadataRepositoryImpl implements EntityMetadataRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    private static final String FIND_METADATA_SQL = """
        SELECT
            e.id AS entity_id,
            e.name AS entity_name,
            c.name AS category_name
        FROM entity e
        JOIN entity_category ec ON ec.entity_id = e.id AND ec.active = TRUE
        JOIN category c ON c.id = ec.category_id AND c.active = TRUE
        WHERE e.id IN (:entityIds)
        """;

    private static final RowMapper<EntityMetadata> ROW_MAPPER = (rs, rowNum) ->
        new EntityMetadata(
            rs.getObject("entity_id", UUID.class),
            rs.getString("entity_name"),
            rs.getString("category_name")
        );

    @Override
    public Map<UUID, EntityMetadata> findByEntityIds(List<UUID> entityIds) {
        if (CollectionUtils.isEmpty(entityIds)) {
            return Map.of();
        }

        List<EntityMetadata> metadata = jdbcTemplate.query(
            FIND_METADATA_SQL,
            Map.of("entityIds", entityIds),
            ROW_MAPPER
        );

        return metadata.stream()
            .collect(Collectors.toMap(
                EntityMetadata::id,
                value -> value,
                (existing, ignored) -> existing
            ));
    }
    
}
