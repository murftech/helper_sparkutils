git init
git add .
git commit -m "initial commit: sparkutils package (getspark, functions)"   
gh repo create helper_sparkutils --public --source=. --remote=origin --push 

git remote -v
gh repo view --web