@echo off
cd ignite
set PR_ID=%1%
set APACHE_GIT="https://git-wip-us.apache.org/repos/asf/ignite"
set GITHUB_MIRROR="https://github.com/apache/ignite.git"
set TARGET_BRANCH="master"

if not "%PR_ID%" == "" goto fetch_pr

echo "You have to specify 'pull-request-id'."
goto finish

:fetch_pr

set PR_BRANCH_NAME="pull-%PR_ID%-head"

git fetch %GITHUB_MIRROR% pull/%PR_ID%/head:%PR_BRANCH_NAME%

FOR /F "tokens=*" %%g IN ('git --no-pager show -s "--format=%%aN <%%aE>" "%PR_BRANCH_NAME%"') do (SET AUTHOR=%%g)
FOR /F "tokens=*" %%g IN ('git --no-pager show -s "--format=%%B" "%PR_BRANCH_NAME%"') do (SET ORIG_COMMENT=%%g)

echo Author: "%AUTHOR%"
echo Original comment is "%ORIG_COMMENT%"

set /p COMMENT="Press [ENTER] if you're agree with the comment or type your comment and press [ENTER]:":

if "%COMMENT%" == "" set COMMENT=%ORIG_COMMENT% 

set COMMENT=%COMMENT% - Fixes #%PR_ID%.

echo New comment is: "%COMMENT%"

git pull %GITHUB_MIRROR% pull/%PR_ID%/head --squash

git commit --author "%AUTHOR%" -a -s -m "%COMMENT%"

echo Squash commit for pull request with id='%PR_ID%' has been added. The commit has been added with comment '%COMMENT%'.
echo Now you can review changes of the last commit at %TARGET_BRANCH% and push it into %APACHE_GIT% git after.
echo If you want to decline changes, you can remove the last commit from your repo by 'git reset --hard HEAD^'.

rem Clean-up.
git branch -D %PR_BRANCH_NAME% > nul

echo Successfully completed.

:finish
cd ..