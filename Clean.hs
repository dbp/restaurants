{-# LANGUAGE OverloadedStrings #-}

import Control.Lens
import Control.Applicative
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as L8
import qualified Data.Text.Lazy as T
import qualified Data.Text.Lazy.Encoding as E

-- NOTE(dbp 2014-06-15): There are invalid UTF-8 sequences, which I'm just eating.
main = do file <- E.decodeUtf8With (\_msg _val -> Nothing) <$> L.readFile "WebExtract.txt"
          let clean = map (T.intercalate "\",\"" . cleanLine . T.splitOn "\",\"") (T.splitOn "\n" file)
          L.writeFile "WebExtractClean.txt" $ E.encodeUtf8 $ T.intercalate "\n" clean
  where cleanLine ls = ls & ix 1 %~ (T.replace "\"" "'")
