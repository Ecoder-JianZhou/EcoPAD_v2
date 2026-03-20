1. copy site-template to your name
2. modify Dockerfile
    "EXPOSE 8011" and "--port", "8011" to new port, such as 8012
3. modify docker-compose.yml
    attention: name and port
4. modify "config", including site.json, models
5. add your site info into "runner/config/sites.json"
