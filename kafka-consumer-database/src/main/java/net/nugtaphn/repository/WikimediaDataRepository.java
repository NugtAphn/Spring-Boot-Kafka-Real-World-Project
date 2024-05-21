package net.nugtaphn.repository;

import net.nugtaphn.entity.WikimediaData;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WikimediaDataRepository extends JpaRepository<WikimediaData, Long> {
}