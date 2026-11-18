# 1. Image de base officielle légère et stable
FROM python:3.11-slim

# 2. Variables d'environnement d'optimisation Python
# PYTHONDONTWRITEBYTECODE=1 : Empêche Python de créer des fichiers .pyc inutiles en conteneur
# PYTHONUNBUFFERED=1 : Force l'affichage des logs (print) en temps réel dans la console Docker
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. Dossier de travail dans le conteneur
WORKDIR /app

# 4. Installation des dépendances en premier (Exploitation du cache des couches Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copie du code source du microservice
COPY . .

# 6. Bonne pratique Sécurité (DevSecOps) : Ne jamais exécuter l'application en tant que 'root'
RUN adduser --disabled-password --gecos "" appuser && chown -R appuser:appuser /app
USER appuser

# 7. Exposition du port documentaire et commande de lancement
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]