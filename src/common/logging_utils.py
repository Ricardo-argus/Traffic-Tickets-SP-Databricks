"""Configuração padrão de logging do pipeline."""

import logging

# Mesmo formato já usado pelos três scripts (basicConfig é idempotente:
# a primeira chamada configura, as demais não alteram nada)
FORMATO_LOG = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging(nome: str, level: int = logging.INFO) -> logging.Logger:
    """Garante o formato padrão de logging e devolve um logger nomeado."""
    logging.basicConfig(level=level, format=FORMATO_LOG)
    return logging.getLogger(nome)
