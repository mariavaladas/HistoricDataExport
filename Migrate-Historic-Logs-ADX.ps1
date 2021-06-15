<#
MIT License

Copyright (c) 2021 andedevsecops

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

#>

PARAM (
    [Parameter(Mandatory=$true)] $LogAnalyticsWorkspaceName,
    [Parameter(Mandatory=$true)] $LogAnalyticsResourceGroup,     
    [Parameter(Mandatory=$true)] $EventHubNamespace,
    [Parameter(Mandatory=$true)] $EventHubNamespaceResourceGroup,
    [Parameter(Mandatory=$true)] $TableName,
    [Parameter(Mandatory=$true)] $startperiod,
    [Parameter(Mandatory=$true)] $endperiod,   

    $rowLimit = 2500,
    $GLOBAL:DATERANGETOTALLOGSIZE = 0,
    $GLOBAL:DATERANGEEVENTHUBTOPICROWS = 0,
            
    $EventHubTopicName = "historic-$($TableName.ToLower())",    
    $EventHubTopicFlag = $false      
)
Function Write-Log {
    [CmdletBinding()]
    param(
        [Parameter()]
        [ValidateNotNullOrEmpty()]
        [string]$Message,
        [string]$LogFileName,
 
        [Parameter()]
        [ValidateNotNullOrEmpty()]
        [ValidateSet('Information','Warning','Error')]
        [string]$Severity = 'Information'
    )
    try {
        [pscustomobject]@{
            Time = (Get-Date -f g)
            Message = $Message
            Severity = $Severity
        } | Export-Csv -Path "$PSScriptRoot\$LogFileName" -Append -NoTypeInformation
    }
    catch {
        Write-Host "An error occured in Write-Log() method" -ForegroundColor Red
    }
    
}

Function PostMessagesToEventHubTopic{
    param(
    $EventHubSASToken,
    $TableRows,
    $EventHubNamespace,
    $HistoricEventHubTopic
    )
        
    $joinRows = $TableRows -join ","      
    $payload = '{"records": [' + $joinRows + ']}'
       
    Write-Host "Calling Rest API to send data to event hub topic $HistoricEventHubTopic..."
    Write-Log -Message "Calling Rest API to send data to event hub topic $HistoricEventHubTopic..." -LogFileName $LogFileName -Severity Information
    $eventHubEndPoint = "https://$EventHubNamespace.servicebus.windows.net/$HistoricEventHubTopic/messages?timeout=60?api-version=2014-01"    
    $sasTokenValue = $EventHubSASToken.SharedAccessSignature
    $eventHubHeaders = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
    $eventHubHeaders.Add("Content-Type", "application/atom+xml;type=entry;charset=utf-8")
    $eventHubHeaders.Add("Authorization", "SharedAccessSignature$sasTokenValue")    

    try {        
        $restInvokeResult = Invoke-RestMethod -Uri $eventHubEndPoint -Method "POST" -Headers $eventHubHeaders -Body $payload -Verbose        
        Write-Host $restInvokeResult
    } catch {    
        Write-Host "StatusCode:" $_.Exception.Response.StatusCode.value__ -ForegroundColor Red
        Write-Log -Message $_.Exception.Response.StatusCode.value__ -LogFileName $LogFileName -Severity Error
        Write-Host "StatusDescription:" $_.Exception.Response.StatusDescription -ForegroundColor Red
        Write-Log -Message $_.Exception.Response.StatusDescription -LogFileName $LogFileName -Severity Error
    }
}


Function QueryLogAnalyticsWithLimits {  
    Param(
    $TableName, 
    $startperiod, 
    $endperiod, 
    $LogAnalyticsWorkspaceId    
    )
    
    $query = "$($TableName)| where TimeGenerated between (todatetime('$startperiod')..todatetime('$endperiod')) | sort by TimeGenerated asc | take $rowLimit"
            
    try {
        Write-Host "Executing query:$query on Log Analytics table $TableName" -ForegroundColor Green
        Write-Log -Message "Executing query:$query on Log Analytics table $TableName" -LogFileName $LogFileName -Severity Information                    
        $queryResults = (Invoke-AzOperationalInsightsQuery -WorkspaceId $LogAnalyticsWorkspaceId -Query $query).Results
        
        return $queryResults
    }
    catch {
        Write-Host "Performance hit - please reduce row limit" -ForegroundColor Red
        Write-Host "Error:$($_.ErrorDetails.Message) Command:$($_.InvocationInfo.Line)" -ForegroundColor Red
                            
        Write-Log -Message "Performance hit - please reduce row limit" -LogFileName $LogFileName -Severity Error
        Write-Log -Message "Error:$($_.ErrorDetails.Message) Command:$($_.InvocationInfo.Line)" -LogFileName $LogFileName -Severity Error
    }
}
Function ProcessQueryResults {  
    Param(
    $LogAData,
    $EventHubSasToken
    )    
        
    try{
        if ($EventHubTopicFlag) {            
            $logsArray = @()            
            $total_size_sent = 0
            $current_size = 0
            $EventHubTopicRowCount = 0
            $EventHubDataLimit = 1024 * 1024 - 3000
            $resultsJson = $LogAData | ConvertTo-Json -Compress -Depth 20            
            $LogAData_size = [System.Text.Encoding]::UTF8.GetByteCount($resultsJson)

            if ($LogAData_size -gt $EventHubDataLimit) {
                Write-Host "Query results size: $LogAData_size Bytes exceeded EventHub allowed size $EventHubDataLimit from Table: $TableName; Splitting the Data"
                Write-Log -Message "Query results size: $LogAData_size Bytes exceeded EventHub allowed size $EventHubDataLimit from Table: $TableName; Splitting the Data" -LogFileName $LogFileName -Severity Information  
                foreach ($rowData in $LogAData) {
                    $rowData_json = $rowData | ConvertTo-Json -Compress -Depth 20
                    $rowData_size = [System.Text.Encoding]::UTF8.GetByteCount($rowData_json)
                    $current_size = $current_size+$rowData_size
                    $logsArray = $logsArray + $rowData_json    
                    if ($current_size -gt $EventHubDataLimit) 
                    {
                        PostMessagesToEventHubTopic -EventHubSASToken $EventHubSasToken `
                        -TableRows $logsArray `
                        -EventHubNamespace $EventHubNamespace `
                        -HistoricEventHubTopic $EventHubTopicName
                        
                        $EventHubTopicRowCount++                        
                        $total_size_sent = $total_size_sent + $current_size                        
                        $current_size = 0
                        $logsArray = @()   
                        
                        Write-Host "Query results size: $total_size_sent Bytes sent to EventHub Topic $EventHubTopicName from Table: $TableName; Rows:$EventHubTopicRowCount"
                        Write-Log -Message "Query results size: $total_size_sent Bytes sent to EventHub Topic $EventHubTopicName from Table: $TableName; Rows:$EventHubTopicRowCount" -LogFileName $LogFileName -Severity Information                
                    }
                }
                $total_size_sent = $total_size_sent + $current_size
                                
                PostMessagesToEventHubTopic -EventHubSASToken $EventHubSasToken `
                        -TableRows $logsArray `
                        -EventHubNamespace $EventHubNamespace `
                        -HistoricEventHubTopic $EventHubTopicName
                
                $EventHubTopicRowCount++

                Write-Host "Left over query results size:$current_size; Rows:$EventHubTopicRowCount" -ForegroundColor Green
                Write-Log -Message "Left over query results size:$current_size; Rows:$EventHubTopicRowCount" -LogFileName $LogFileName -Severity Information  
                        
                
                $lastTimeGenerated = $logsArray[$logsArray.Count - 1]
                $startperiod = ($lastTimeGenerated | ConvertFrom-Json).TimeGenerated
            } 
            else {                
                foreach ($rowData in $LogAData) {
                    $rowData_json = $rowData | ConvertTo-Json
                    $logsArray = $logsArray+$rowData_json
                }
                $total_size_sent = $LogAData_size                        
                PostMessagesToEventHubTopic -EventHubSASToken $EventHubSasToken `
                        -TableRows $logsArray `
                        -EventHubNamespace $EventHubNamespace `
                        -HistoricEventHubTopic $EventHubTopicName               
                
                $EventHubTopicRowCount++                                
                $lastTimeGenerated = $logsArray[$logsArray.Count - 1]
                $startperiod = ($lastTimeGenerated | ConvertFrom-Json).TimeGenerated
            }
        }
        else {
            Write-Host "EventHub Topic $EventHubTopicName not found in $EventHubNamespaceResourceGroup" -ForegroundColor Red
            Write-Log -Message "EventHub Topic $EventHubTopicName not found in $EventHubNamespaceResourceGroup" -LogFileName $LogFileName -Severity Error                
        }                               
    }
    catch {
        Write-Host "An error occured in retreiving EventHub Topics from $EventHubNamespaceResourceGroup" -ForegroundColor Red
        Write-Log -Message "An error occured in retreiving EventHub Topics from $EventHubNamespaceResourceGroup" -LogFileName $LogFileName -Severity Error        
    }     
    
    return $startperiod, $total_size_sent, $EventHubTopicRowCount
}


$TimeStamp = Get-Date -Format yyyyMMdd_HHmmss
$LogFileName = '{0}_{1}.csv' -f "HistoricDataMigration", $TimeStamp

Write-Host "`r`nIf not logged in to Azure already, you will now be asked to log in to your Azure environment. `nFor this script to work correctly, you need to provide credentials `nAzure Log Analytics Workspace Read Permissions `nAzure Data Explorer Database User Permission. " -BackgroundColor Blue

Read-Host -Prompt "Press enter to continue or CTRL+C to quit the script"

$context = Get-AzContext

if(!$context){  
    Connect-AzAccount
    $context = Get-AzContext
}

$SubscriptionId = $context.Subscription.Id


try {
    $WorkspaceObject = Get-AzOperationalInsightsWorkspace -Name $LogAnalyticsWorkspaceName -ResourceGroupName $LogAnalyticsResourceGroup -DefaultProfile $context 
    $LogAnalyticsLocation = $WorkspaceObject.Location
    $LogAnalyticsWorkspaceId = $WorkspaceObject.CustomerId
    Write-Host "Workspace named $LogAnalyticsWorkspaceName in region $LogAnalyticsLocation exists."  -ForegroundColor Green
    Write-Log -Message "Workspace named $LogAnalyticsWorkspaceName in region $LogAnalyticsLocation exists." -LogFileName $LogFileName -Severity Information
} catch {
    Write-Host "$LogAnalyticsWorkspaceName not found"
    Write-Log -Message "$LogAnalyticsWorkspaceName not found" -LogFileName $LogFileName -Severity Error
}

#Check EventHub Topic Exists
$isEventHubTopicExists = Get-AzEventHub -ResourceGroup $EventHubNamespaceResourceGroup `
                                            -NamespaceName $EventHubNamespace `
                                            -EventHubName $EventHubTopicName `
                                            -ErrorAction SilentlyContinue `
                                            -Verbose

if ($null -eq $isEventHubTopicExists) {
    $newEventHubTopic = New-AzEventHub -ResourceGroupName $EventHubNamespaceResourceGroup `
    -NamespaceName $EventHubNamespace `
    -Name $EventHubTopicName `
    -PartitionCount 32 `
    -MessageRetentionInDays 1 `
    -Verbose `
    -ErrorAction SilentlyContinue

    if ($newEventHubTopic.Status -eq "Active") {
        $EventHubTopicFlag = $true
    }
}
else {
    $EventHubTopicFlag = $true
}

#Generate EventHubTopic SAS Token
if ($EventHubTopicFlag) {
    $authRuleName = "$($TableName)AuthRule"

    $isAuthRuleExists = Get-AzEventHubAuthorizationRule -ResourceGroupName $EventHubNamespaceResourceGroup `
                    -NamespaceName $EventHubNamespace `
                    -EventHubName $EventHubTopicName `
                    -ErrorAction SilentlyContinue `
                    -Verbose

    if($null -ne $isAuthRuleExists){
        $AuthorizationRule = $isAuthRuleExists
    } else {
        $AuthorizationRule = New-AzEventHubAuthorizationRule -ResourceGroupName $EventHubNamespaceResourceGroup `
                     -NamespaceName $EventHubNamespace `
                     -EventHubName $EventHubTopicName `
                     -AuthorizationRuleName $authRuleName `
                     -Rights @("Manage","Listen","Send")
    }

    $SasTokenStartTime = Get-Date
    $SasTokenEndTime = $SasTokenStartTime.AddHours(9.0)

    $SasToken = New-AzEventHubAuthorizationRuleSASToken -AuthorizationRuleId $AuthorizationRule.Id `
            -KeyType $authRuleName `
            -ExpiryTime $SasTokenEndTime            
}

$transferStartTime = Get-Date
$actualStartperiod = $startperiod
DO {
    Write-Host "Moving historic Data from $TableName from $startperiod to $endperiod" -ForegroundColor Green
    Write-Log -Message "Moving historic Data from $TableName from $startperiod to $endperiod" -LogFileName $LogFileName -Severity Information
    try {
        $laLogs = QueryLogAnalyticsWithLimits  -TableName $TableName `
            -startperiod $startperiod `
            -endperiod $endperiod `
            -LogAnalyticsWorkspaceId $LogAnalyticsWorkspaceId
        
        if($laLogs.Count -gt 1){            
            $newStart, $totalBytesSize, $totalRowsCount = ProcessQueryResults -LogAData $laLogs `
                    -EventHubSasToken $SasToken          
        }
        $startperiod = $newStart
        $DATERANGETOTALLOGSIZE += $totalBytesSize
        $DATERANGEEVENTHUBTOPICROWS += $totalRowsCount
    }
    catch {
        Write-Host "Error in historic data transfer from $TableName between $startperiod to $endperiod" -ForegroundColor Red
        Write-Log -Message "Error in historic data transfer from $TableName between $startperiod to $endperiod" -LogFileName $LogFileName -Severity Error
        
        Write-Host "Error : $_.ErrorDetails.Message"
        Write-Log -Message "Error : $($_.ErrorDetails.Message)" -LogFileName $LogFileName -Severity Error
        Write-Host "Command : $_.InvocationInfo.Line"
        Write-Log -Message "Command : $($_.InvocationInfo.Line)" -LogFileName $LogFileName -Severity Error
    }   
            
} While ($startperiod -le $endperiod)
$transferEndTime = Get-Date
$totalTransferTime = $transferEndTime - $transferStartTime
Write-Host "Success!!! Date between $actualStartperiod and $endperiod sent to EventHub Topic; Total Transfer time:$totalTransferTime;Total size sent:$DATERANGETOTALLOGSIZE Bytes; Transfer Start Time:$transferStartTime; Transfer End Time:$transferEndTime;  Total Rows:$DATERANGEEVENTHUBTOPICROWS" -ForegroundColor Green
Write-Log -Message "Success!!! Date between $actualStartperiod and $endperiod sent to EventHub Topic; Total Transfer time:$totalTransferTime;Total size sent:$DATERANGETOTALLOGSIZE Bytes; Transfer Start Time:$transferStartTime; Transfer End Time:$transferEndTime;  Total Rows:$DATERANGEEVENTHUBTOPICROWS" -LogFileName $LogFileName -Severity Information