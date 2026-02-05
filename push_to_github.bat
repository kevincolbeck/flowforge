@echo off
echo ========================================
echo FlowForge - Pushing to GitHub
echo ========================================
echo.

cd C:\Users\kevin\projects\universal-integrator

echo Creating GitHub repository...
echo You'll be prompted for authentication.
echo.

curl -u kevincolbeck https://api.github.com/user/repos -d "{\"name\":\"flowforge\",\"description\":\"Universal API Integration Platform - Better than Zapier\",\"private\":false}"

echo.
echo Adding remote and pushing...
git remote add origin https://github.com/kevincolbeck/flowforge.git
git branch -M main
git push -u origin main

echo.
echo ========================================
echo Done! Repository at:
echo https://github.com/kevincolbeck/flowforge
echo ========================================
echo.
echo Next: Deploy to Railway at https://railway.app
pause
